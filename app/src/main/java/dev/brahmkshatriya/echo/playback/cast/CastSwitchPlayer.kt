package dev.brahmkshatriya.echo.playback.cast

import android.content.Context
import android.net.wifi.WifiManager
import android.os.Handler
import android.os.PowerManager
import android.os.SystemClock
import androidx.annotation.OptIn
import androidx.core.net.toUri
import androidx.media3.cast.CastPlayer
import androidx.media3.cast.MediaItemConverter
import androidx.media3.cast.SessionAvailabilityListener
import androidx.media3.common.C
import androidx.media3.common.ForwardingSimpleBasePlayer
import androidx.media3.common.MediaItem
import androidx.media3.common.Player
import androidx.media3.common.util.UnstableApi
import androidx.media3.common.util.Util
import com.google.android.gms.cast.MediaInfo
import com.google.android.gms.cast.MediaLoadRequestData
import com.google.android.gms.cast.MediaMetadata
import com.google.android.gms.cast.MediaQueueData
import com.google.android.gms.cast.MediaQueueItem
import com.google.android.gms.cast.MediaStatus
import com.google.android.gms.cast.framework.CastContext
import com.google.android.gms.cast.framework.media.MediaQueue
import com.google.android.gms.cast.framework.media.RemoteMediaClient
import com.google.android.gms.common.ConnectionResult
import com.google.android.gms.common.GoogleApiAvailability
import com.google.android.gms.common.api.PendingResult
import com.google.android.gms.common.images.WebImage
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import dev.brahmkshatriya.echo.di.App
import dev.brahmkshatriya.echo.download.Downloader
import dev.brahmkshatriya.echo.extensions.ExtensionLoader
import dev.brahmkshatriya.echo.playback.MediaItemUtils.retries
import dev.brahmkshatriya.echo.playback.MediaItemUtils.serverIndex
import dev.brahmkshatriya.echo.playback.MediaItemUtils.sourceIndex
import dev.brahmkshatriya.echo.playback.PlayerState
import dev.brahmkshatriya.echo.playback.exceptions.PlayerException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface

/**
 * Plays on the local [ExoPlayer][androidx.media3.exoplayer.ExoPlayer] and hands the queue to a
 * [CastPlayer] while a Cast session is connected, like Media3 1.9's local/remote `CastPlayer`.
 *
 * Media3 1.8's [CastPlayer] only knows the few items the receiver reports, and it applies queue
 * changes asynchronously. So we keep the full queue here. That gives
 * [dev.brahmkshatriya.echo.playback.ShufflePlayer] and the listeners the same synchronous
 * playlist as with ExoPlayer, and every change is mirrored to the receiver by item id.
 */
@OptIn(UnstableApi::class)
class CastSwitchPlayer private constructor(
    context: Context,
    private val localPlayer: Player,
    castContext: CastContext,
    private val app: App,
    private val scope: CoroutineScope,
    state: PlayerState,
    extensions: ExtensionLoader,
    downloadFlow: StateFlow<List<Downloader.Info>>,
    private val mediaChangeFlow: MutableSharedFlow<Pair<MediaItem, MediaItem>>,
) : ForwardingSimpleBasePlayer(localPlayer), CastStreamServer.Listener {

    private val server = CastStreamServer(app, state, extensions, downloadFlow, this)
    private val sessionManager = castContext.sessionManager
    private val castPlayer = CastPlayer(
        context, castContext, ItemConverter(),
        C.DEFAULT_SEEK_BACK_INCREMENT_MS,
        C.DEFAULT_SEEK_FORWARD_INCREMENT_MS,
        C.DEFAULT_MAX_SEEK_TO_PREVIOUS_POSITION_MS
    )
    private val handler = Handler(localPlayer.applicationLooper)
    private val locks = NetworkLocks(context)

    private val isRemote get() = player === castPlayer
    private val remoteClient get() = sessionManager.currentCastSession?.remoteMediaClient

    private class Entry(var item: MediaItem, var token: String) {
        // Stable identity for the Media3 playlist, since item and token can change.
        val uid = Any()
        // The receiver's id for this entry, learned in reconcile().
        var itemId = MediaQueueItem.INVALID_ITEM_ID
        var sent = false
        val hasId get() = itemId != MediaQueueItem.INVALID_ITEM_ID
    }

    private fun newEntry(item: MediaItem) = Entry(item, server.register(item))

    // The full queue while casting. Entries are compared by identity.
    private val queue = mutableListOf<Entry>()
    // Last index we showed, the fallback when the receiver reports nothing we recognise.
    private var lastIndex = 0
    // ShufflePlayer does the shuffling, we only remember the flag for the way back to the phone.
    private var shuffleModeEnabled = localPlayer.shuffleModeEnabled
    // Why a track failed, by token, so the skip can report the real error.
    private val failures = mutableMapOf<String, Throwable>()
    // Entries already skipped, so one failure is reported once.
    private val skippedTokens = mutableSetOf<String>()

    // A load or jump the receiver has not reported yet. Until it does, the UI shows the target
    // track instead of snapping back to the old one.
    private class Pending(
        val entry: Entry,
        val positionMs: Long,
        val playWhenReady: Boolean?,
        val deadline: Long = SystemClock.elapsedRealtime() + PENDING_TIMEOUT_MS,
    )

    private var pending: Pending? = null

    private sealed interface Op
    private class Load(val entries: List<Entry>, val positionMs: Long, val autoplay: Boolean) : Op
    private class Insert(val entries: List<Entry>) : Op
    private class Remove(val entries: List<Entry>) : Op
    private class Move(val entries: List<Entry>) : Op
    private class Jump(val entry: Entry, val positionMs: Long) : Op

    // Changes not sent to the receiver yet, in order. Most wait for the receiver to report the
    // item ids of the entries they refer to.
    private val ops = ArrayDeque<Op>()
    private var watchedOp: Op? = null
    private var loadsInFlight = 0
    private var lastResync = 0L

    private val sessionListener = object : SessionAvailabilityListener {
        override fun onCastSessionAvailable() = connect()
        override fun onCastSessionUnavailable() = disconnect()
    }

    private val castListener = object : Player.Listener {
        override fun onEvents(player: Player, events: Player.Events) {
            if (!isRemote) return
            locks.setHeld(castPlayer.playWhenReady)
            checkReceiverError()
        }
    }

    private var registeredQueue: MediaQueue? = null
    private val queueCallback = object : MediaQueue.Callback() {
        override fun mediaQueueChanged() {
            if (!isRemote) return
            // New ids may be what a waiting op needed.
            flushOps()
            invalidateState()
        }
    }

    private val flushRunnable = Runnable { flushOps() }
    private val pendingTimeout = Runnable { invalidateIfRemote() }

    // If the receiver never reports the ids an operation waits for, start again from our queue.
    private val watchdog = Runnable { resync() }
    private var connectJob: Job? = null

    init {
        castPlayer.addListener(castListener)
        castPlayer.setSessionAvailabilityListener(sessionListener)
        if (castPlayer.isCastSessionAvailable) connect()
    }

    // Session switching

    private fun connect() {
        if (isRemote) return
        val device = sessionManager.currentCastSession?.castDevice ?: return
        connectJob?.cancel()
        connectJob = scope.launch(Dispatchers.IO) {
            // The receiver downloads from us, so the server has to be up before anything loads.
            runCatching {
                val address = localAddressFor(device.inetAddress)
                    ?: throw Exception("Could not find this device's address on the local network")
                server.start(address)
            }.onFailure {
                app.throwFlow.emit(it)
                return@launch
            }
            withContext(Dispatchers.Main) {
                // The session may have ended while the server was starting.
                if (castPlayer.isCastSessionAvailable) transferToRemote() else server.stop()
            }
        }
    }

    private fun transferToRemote() {
        if (isRemote) return
        val items = List(localPlayer.mediaItemCount) { localPlayer.getMediaItemAt(it) }
        val index = localPlayer.currentMediaItemIndex
        val position = localPlayer.currentPosition
        val playWhenReady = localPlayer.playWhenReady
        shuffleModeEnabled = localPlayer.shuffleModeEnabled
        castPlayer.repeatMode = localPlayer.repeatMode

        queue.clear()
        items.mapTo(queue) { newEntry(it) }
        lastIndex = index
        failures.clear()
        skippedTokens.clear()
        registeredQueue = remoteClient?.mediaQueue?.also { it.registerCallback(queueCallback) }
        if (queue.isNotEmpty()) load(index, position, playWhenReady)

        setPlayer(castPlayer)
        localPlayer.stop()
        locks.setHeld(playWhenReady)
    }

    private fun disconnect() {
        connectJob?.cancel()
        if (!isRemote) return server.stop()
        val entries = queue.toList()
        val index = currentMediaItemIndex
        val position = currentPosition

        ops.clear()
        pending = null
        loadsInFlight = 0
        queue.clear()
        registeredQueue?.unregisterCallback(queueCallback)
        registeredQueue = null

        // Shuffle and repeat were tracked here while casting, give them back to the phone.
        localPlayer.shuffleModeEnabled = shuffleModeEnabled
        localPlayer.repeatMode = castPlayer.repeatMode
        if (entries.isEmpty()) localPlayer.clearMediaItems() else {
            localPlayer.setMediaItems(
                entries.map { it.item }, index.coerceIn(0, entries.lastIndex), position
            )
            // Come back paused, the phone should not start playing on its own.
            localPlayer.playWhenReady = false
            localPlayer.prepare()
        }
        setPlayer(localPlayer)
        server.stop()
        locks.setHeld(false)
    }

    // State

    override fun getState(): State {
        val state = super.getState()
        if (!isRemote) return state
        // Refresh the item ids on every state read, they are what the ops wait for.
        remoteClient?.let { reconcile(it) }
        if (ops.isNotEmpty()) {
            // Posted so we do not send requests while in the middle of building a state.
            handler.removeCallbacks(flushRunnable)
            handler.post(flushRunnable)
        }

        val builder = state.buildUpon()
            // The CastPlayer has no shuffle of its own, so report the flag we keep.
            .setShuffleModeEnabled(shuffleModeEnabled)
            .setAvailableCommands(
                state.availableCommands.buildUpon().addAll(*REMOTE_COMMANDS).build()
            )
        if (queue.isEmpty()) {
            val ended = state.playbackState == Player.STATE_ENDED
            return builder.setPlaylist(listOf())
                .setCurrentMediaItemIndex(C.INDEX_UNSET)
                .setPlaybackState(if (ended) Player.STATE_ENDED else Player.STATE_IDLE)
                .build()
        }

        val receiverEntry = receiverEntry()
        // A target stops being pending once the receiver reports it, it leaves the queue, or
        // its deadline passes.
        val pending = pending?.takeIf {
            it.entry !== receiverEntry && it.entry in queue
                    && SystemClock.elapsedRealtime() < it.deadline
        }
        this.pending = pending
        // While a target is pending, show it rather than what the receiver still reports.
        val current = pending?.entry ?: receiverEntry
        val index = current?.let { queue.indexOf(it) }?.takeIf { it >= 0 }
            ?: lastIndex.coerceIn(0, queue.lastIndex)
        lastIndex = index

        // The receiver's duration only applies to the entry it is playing.
        val currentDurationUs = if (pending == null && receiverEntry === queue[index])
            Util.msToUs(castPlayer.contentDuration) else C.TIME_UNSET
        val playlist = queue.mapIndexed { i, entry ->
            MediaItemData.Builder(entry.uid)
                .setMediaItem(entry.item)
                .setIsSeekable(true)
                .setDurationUs(if (i == index) currentDurationUs else C.TIME_UNSET)
                .build()
        }
        builder.setPlaylist(playlist).setCurrentMediaItemIndex(index)
        if (pending != null) {
            // Show buffering at the target position until the receiver catches up.
            builder.setPlaybackState(Player.STATE_BUFFERING)
                .setContentPositionMs(pending.positionMs)
                .setContentBufferedPositionMs(PositionSupplier.getConstant(pending.positionMs))
                .setTotalBufferedDurationMs(PositionSupplier.ZERO)
            pending.playWhenReady?.let {
                builder.setPlayWhenReady(it, Player.PLAY_WHEN_READY_CHANGE_REASON_USER_REQUEST)
            }
            // Refresh the state when the deadline passes, so a target the receiver never reaches
            // does not stay on screen.
            handler.removeCallbacks(pendingTimeout)
            handler.postDelayed(pendingTimeout, pending.deadline - SystemClock.elapsedRealtime())
        }
        return builder.build()
    }

    private fun invalidateIfRemote() {
        if (isRemote) invalidateState()
    }

    private fun receiverToken() = server.tokenOf(remoteClient?.mediaStatus?.mediaInfo?.contentId)
    private fun receiverEntry() = receiverToken()?.let { token -> queue.find { it.token == token } }

    // Commands

    override fun handleSetPlayWhenReady(playWhenReady: Boolean): ListenableFuture<*> {
        if (isRemote && playWhenReady && isReceiverIdle()) reload(true)
        return super.handleSetPlayWhenReady(playWhenReady)
    }

    override fun handlePrepare(): ListenableFuture<*> {
        if (!isRemote) return super.handlePrepare()
        if (isReceiverIdle()) reload(playWhenReady)
        return Futures.immediateVoidFuture()
    }

    override fun handleStop(): ListenableFuture<*> {
        if (!isRemote) return super.handleStop()
        ops.clear()
        pending = null
        queue.forEach { it.sent = false; it.itemId = MediaQueueItem.INVALID_ITEM_ID }
        return super.handleStop()
    }

    override fun handleRelease(): ListenableFuture<*> {
        // Detach everything first so nothing fires into a released player.
        connectJob?.cancel()
        handler.removeCallbacksAndMessages(null)
        castPlayer.setSessionAvailabilityListener(null)
        castPlayer.removeListener(castListener)
        registeredQueue?.unregisterCallback(queueCallback)
        castPlayer.release()
        localPlayer.release()
        server.release()
        locks.setHeld(false)
        return Futures.immediateVoidFuture()
    }

    override fun handleSetShuffleModeEnabled(shuffleModeEnabled: Boolean): ListenableFuture<*> {
        // ShufflePlayer reorders the queue itself and Media3's CastPlayer ignores the mode, so we
        // just remember it.
        this.shuffleModeEnabled = shuffleModeEnabled
        if (!isRemote) return super.handleSetShuffleModeEnabled(shuffleModeEnabled)
        return Futures.immediateVoidFuture()
    }

    override fun handleSetMediaItems(
        mediaItems: MutableList<MediaItem>, startIndex: Int, startPositionMs: Long,
    ): ListenableFuture<*> {
        if (!isRemote) return super.handleSetMediaItems(mediaItems, startIndex, startPositionMs)
        // A whole new queue gets new entries, so new tokens too.
        queue.clear()
        mediaItems.mapTo(queue) { newEntry(it) }
        if (queue.isEmpty()) {
            ops.clear()
            pending = null
            castPlayer.stop()
        } else load(
            // INDEX_UNSET and TIME_UNSET both mean the very start.
            if (startIndex == C.INDEX_UNSET) 0 else startIndex,
            if (startPositionMs == C.TIME_UNSET) 0 else startPositionMs,
            playWhenReady
        )
        return Futures.immediateVoidFuture()
    }

    override fun handleAddMediaItems(
        index: Int, mediaItems: MutableList<MediaItem>,
    ): ListenableFuture<*> {
        if (!isRemote) return super.handleAddMediaItems(index, mediaItems)
        val wasEmpty = queue.isEmpty()
        val entries = mediaItems.map { newEntry(it) }
        queue.addAll(index.coerceAtMost(queue.size), entries)
        // An empty queue means nothing on the receiver to insert into, so it needs a load.
        if (wasEmpty) load(0, 0, playWhenReady) else enqueue(Insert(entries))
        return Futures.immediateVoidFuture()
    }

    override fun handleRemoveMediaItems(fromIndex: Int, toIndex: Int): ListenableFuture<*> {
        if (!isRemote) return super.handleRemoveMediaItems(fromIndex, toIndex)
        val range = queue.subList(fromIndex, toIndex.coerceAtMost(queue.size))
        // Copy before clearing, the sublist is a view and the Remove op needs the entries.
        val removed = range.toList()
        range.clear()
        if (queue.isEmpty()) {
            ops.clear()
            pending = null
            castPlayer.stop()
        } else enqueue(Remove(removed))
        return Futures.immediateVoidFuture()
    }

    override fun handleMoveMediaItems(
        fromIndex: Int, toIndex: Int, newIndex: Int,
    ): ListenableFuture<*> {
        if (!isRemote) return super.handleMoveMediaItems(fromIndex, toIndex, newIndex)
        val range = queue.subList(fromIndex, toIndex.coerceAtMost(queue.size))
        val moved = range.toList()
        range.clear()
        queue.addAll(newIndex.coerceAtMost(queue.size), moved)
        // Our queue is already reordered, the receiver gets the same move by item id later.
        enqueue(Move(moved))
        return Futures.immediateVoidFuture()
    }

    override fun handleReplaceMediaItems(
        fromIndex: Int, toIndex: Int, mediaItems: MutableList<MediaItem>,
    ): ListenableFuture<*> {
        if (!isRemote) return super.handleReplaceMediaItems(fromIndex, toIndex, mediaItems)
        val old = queue.subList(fromIndex, toIndex.coerceAtMost(queue.size)).toList()
        val sameMedia = old.size == mediaItems.size &&
                old.indices.all { canUpdate(old[it].item, mediaItems[it]) }
        if (sameMedia) {
            // Same media, only the metadata changed (a like, say). The receiver keeps playing.
            old.forEachIndexed { i, entry ->
                entry.item = mediaItems[i]
                server.update(entry.token, mediaItems[i])
            }
            return Futures.immediateVoidFuture()
        }
        // Insert the replacements right after the old entries, then remove those. If the current
        // item is replaced, the receiver then moves straight on to the replacement.
        val current = pending?.entry ?: receiverEntry()
        val entries = mediaItems.map { newEntry(it) }
        queue.addAll(fromIndex + old.size, entries)
        queue.subList(fromIndex, fromIndex + old.size).clear()
        enqueue(Insert(entries))
        enqueue(Remove(old))
        // Show the replacement right away instead of flickering back to the old track.
        if (current in old && entries.isNotEmpty()) pending = Pending(entries.first(), 0, null)
        return Futures.immediateVoidFuture()
    }

    override fun handleSeek(
        mediaItemIndex: Int, positionMs: Long, seekCommand: Int,
    ): ListenableFuture<*> {
        if (!isRemote) return super.handleSeek(mediaItemIndex, positionMs, seekCommand)
        val entry = queue.getOrNull(mediaItemIndex) ?: return Futures.immediateVoidFuture()
        val position = if (positionMs == C.TIME_UNSET) 0 else positionMs
        when {
            isReceiverIdle() -> load(mediaItemIndex, position, playWhenReady)
            pending == null && entry === receiverEntry() -> castPlayer.seekTo(position)
            else -> {
                pending = Pending(entry, position, null)
                enqueue(Jump(entry, position))
            }
        }
        return Futures.immediateVoidFuture()
    }

    // Same id, retries and server/source choice means the receiver can keep its item.
    private fun canUpdate(old: MediaItem, new: MediaItem) = runCatching {
        old.mediaId == new.mediaId && old.retries == new.retries &&
                old.serverIndex == new.serverIndex && old.sourceIndex == new.sourceIndex
    }.getOrDefault(false)

    // Receiver queue

    private fun isReceiverIdle() = queue.isNotEmpty() && loadsInFlight == 0 &&
            remoteClient?.mediaStatus?.mediaInfo == null

    private fun reload(autoplay: Boolean) = load(lastIndex, currentPosition, autoplay)

    // Replaces the receiver's queue with ours. Large queues are sent in several messages, as a
    // Cast message cannot be larger than 64KB.
    private fun load(index: Int, positionMs: Long, autoplay: Boolean) {
        if (queue.isEmpty()) return
        ops.clear()
        queue.forEach {
            it.token = server.register(it.item)
            it.itemId = MediaQueueItem.INVALID_ITEM_ID
            it.sent = false
        }
        val start = index.coerceIn(0, queue.lastIndex)
        val end = minOf(start + MAX_ITEMS_PER_MESSAGE, queue.size)
        val entries = queue.toList()
        // Load from the current track so it starts first, then add what follows, then what
        // came before.
        ops.addLast(Load(entries.subList(start, end), positionMs, autoplay))
        if (end < entries.size) ops.addLast(Insert(entries.subList(end, entries.size)))
        if (start > 0) ops.addLast(Insert(entries.subList(0, start)))
        pending = Pending(entries[start], positionMs, autoplay)
        lastIndex = start
        flushOps()
    }

    private fun enqueue(op: Op) {
        // Nothing on the receiver and nothing on its way: skip, the next prepare or play sends
        // the whole queue anyway.
        if (remoteClient?.mediaStatus?.mediaInfo == null && loadsInFlight == 0 && ops.isEmpty())
            return
        ops.addLast(op)
        flushOps()
    }

    private fun flushOps() {
        val client = remoteClient ?: return
        // Fresh ids first, the ops below wait on them.
        reconcile(client)
        while (ops.isNotEmpty()) {
            // Order matters, so stop at the first op that cannot go yet.
            if (!execute(client, ops.first())) break
            ops.removeFirst()
        }
        val head = ops.firstOrNull()
        // Only restart the watchdog when the head changes, or a stuck op would never time out.
        if (head !== watchedOp) {
            handler.removeCallbacks(watchdog)
            watchedOp = head
            if (head != null) handler.postDelayed(watchdog, OP_TIMEOUT_MS)
        }
    }

    private fun resync() {
        if (!isRemote || queue.isEmpty()) return
        val now = SystemClock.elapsedRealtime()
        if (now - lastResync < RESYNC_INTERVAL_MS) {
            handler.removeCallbacks(watchdog)
            handler.postDelayed(watchdog, RESYNC_INTERVAL_MS)
            return
        }
        lastResync = now
        load(currentMediaItemIndex, currentPosition, playWhenReady)
    }

    private fun execute(client: RemoteMediaClient, op: Op): Boolean = when (op) {
        is Load -> {
            val queueData = MediaQueueData.Builder()
                .setItems(op.entries.map { it.toQueueItem() })
                .setStartIndex(0)
                .setRepeatMode(castRepeatMode())
                .build()
            val request = MediaLoadRequestData.Builder()
                .setQueueData(queueData)
                .setAutoplay(op.autoplay)
                .setCurrentTime(op.positionMs)
                .build()
            loadsInFlight++
            client.load(request).setResultCallback {
                loadsInFlight = (loadsInFlight - 1).coerceAtLeast(0)
                if (!it.status.isSuccess) scope.launch {
                    app.throwFlow.emit(Exception("Cast device could not load the queue"))
                }
                invalidateIfRemote()
            }
            op.entries.forEach { it.sent = true }
            true
        }

        is Insert -> {
            val inserted = op.entries.toSet()
            val entries = queue.filter { it in inserted && !it.sent }
            val before = entries.lastOrNull()?.let { receiverEntryAfter(it, inserted) }
            when {
                entries.isEmpty() -> true
                before != null && !before.hasId -> false
                else -> {
                    val beforeId = before?.itemId ?: MediaQueueItem.INVALID_ITEM_ID
                    entries.chunked(MAX_ITEMS_PER_MESSAGE).forEach { chunk ->
                        val items = chunk.map { it.toQueueItem() }.toTypedArray()
                        client.queueInsertItems(items, beforeId, null).resyncOnFailure()
                    }
                    entries.forEach { it.sent = true }
                    true
                }
            }
        }

        is Remove -> {
            // Entries never sent have nothing to remove on the receiver.
            val entries = op.entries.filter { it.sent }
            when {
                entries.isEmpty() -> true
                // Sent but the id has not come back yet, wait for it.
                entries.any { !it.hasId } -> false
                else -> {
                    val ids = entries.map { it.itemId }.toIntArray()
                    client.queueRemoveItems(ids, null).resyncOnFailure()
                    entries.forEach { it.sent = false }
                    true
                }
            }
        }

        is Move -> {
            val moved = op.entries.toSet()
            val entries = queue.filter { it in moved }
            val before = entries.lastOrNull()?.let { receiverEntryAfter(it, moved) }
            when {
                entries.isEmpty() -> true
                // Only items the receiver has reported can be moved.
                entries.any { !it.sent || !it.hasId } -> false
                before != null && !before.hasId -> false
                else -> {
                    val ids = entries.map { it.itemId }.toIntArray()
                    val beforeId = before?.itemId ?: MediaQueueItem.INVALID_ITEM_ID
                    client.queueReorderItems(ids, beforeId, null).resyncOnFailure()
                    true
                }
            }
        }

        is Jump -> when {
            // The target was removed meanwhile, drop the jump.
            op.entry !in queue -> true
            !op.entry.sent || !op.entry.hasId -> false
            else -> {
                client.queueJumpToItem(op.entry.itemId, op.positionMs, null).resyncOnFailure()
                true
            }
        }
    }

    private fun PendingResult<RemoteMediaClient.MediaChannelResult>.resyncOnFailure() =
        setResultCallback { if (!it.status.isSuccess) resync() }

    // The first entry after [entry] that the receiver already has, ignoring [except].
    private fun receiverEntryAfter(entry: Entry, except: Set<Entry>): Entry? {
        val index = queue.indexOf(entry)
        if (index < 0) return null
        for (i in index + 1 until queue.size) {
            val next = queue[i]
            if (next.sent && next !in except) return next
        }
        return null
    }

    // Learns the receiver's item ids for the entries we have sent.
    private fun reconcile(client: RemoteMediaClient) {
        val status = client.mediaStatus
        if (status != null) {
            // Match by the token inside each content URL, it is the only thing both sides share.
            val reported = status.queueItems.map { it.media?.contentId to it.itemId } +
                    (status.mediaInfo?.contentId to status.currentItemId)
            reported.forEach { (contentId, itemId) ->
                val token = server.tokenOf(contentId) ?: return@forEach
                queue.find { it.token == token && it.sent }?.itemId = itemId
            }
        }
        // The receiver lists its item ids in queue order. The list can lag behind a change, so it
        // is only trusted when it agrees with the ids reported with the content ids above.
        val ids = client.mediaQueue.itemIds
        val sent = queue.filter { it.sent }
        if (ids.size != sent.size || sent.none { it.hasId }) return
        if (sent.indices.any { sent[it].hasId && sent[it].itemId != ids[it] }) return
        sent.forEachIndexed { i, entry -> entry.itemId = ids[i] }
    }

    private fun castRepeatMode() = when (castPlayer.repeatMode) {
        Player.REPEAT_MODE_ONE -> MediaStatus.REPEAT_MODE_REPEAT_SINGLE
        Player.REPEAT_MODE_ALL -> MediaStatus.REPEAT_MODE_REPEAT_ALL
        else -> MediaStatus.REPEAT_MODE_REPEAT_OFF
    }

    private fun Entry.toQueueItem(): MediaQueueItem {
        val metadata = MediaMetadata(MediaMetadata.MEDIA_TYPE_MUSIC_TRACK)
        item.mediaMetadata.run {
            title?.let { metadata.putString(MediaMetadata.KEY_TITLE, it.toString()) }
            artist?.let { metadata.putString(MediaMetadata.KEY_ARTIST, it.toString()) }
            albumTitle?.let { metadata.putString(MediaMetadata.KEY_ALBUM_TITLE, it.toString()) }
        }
        if (server.hasArt(item)) metadata.addImage(WebImage(server.artUrl(token).toUri()))
        val url = server.trackUrl(token)
        val info = MediaInfo.Builder(url)
            .setContentUrl(url)
            .setStreamType(MediaInfo.STREAM_TYPE_BUFFERED)
            .setContentType(CastStreamServer.DEFAULT_TYPE)
            .setMetadata(metadata)
            .build()
        return MediaQueueItem.Builder(info)
            .setAutoplay(true)
            .setPreloadTime(PRELOAD_TIME_SECONDS)
            .build()
    }

    // Errors

    private fun checkReceiverError() {
        if (pending != null) return
        val status = remoteClient?.mediaStatus ?: return
        if (status.playerState != MediaStatus.PLAYER_STATE_IDLE) return
        if (status.idleReason != MediaStatus.IDLE_REASON_ERROR) return
        skipFailed(receiverEntry() ?: return)
    }

    // Reports why the current entry failed and moves on to the next one, once per entry.
    private fun skipFailed(entry: Entry) {
        if (!skippedTokens.add(entry.token)) return
        val error = failures[entry.token] ?: Exception("The Cast device could not play this track")
        scope.launch { app.throwFlow.emit(PlayerException(entry.item, error)) }
        val index = queue.indexOf(entry)
        if (index in 0 until queue.lastIndex) seekTo(index + 1, 0)
    }

    override fun onLoaded(token: String, old: MediaItem, new: MediaItem) {
        handler.post {
            val entry = queue.find { it.token == token } ?: return@post
            entry.item = new
            scope.launch { mediaChangeFlow.emit(old to new) }
            invalidateIfRemote()
        }
    }

    override fun onFailed(token: String, item: MediaItem, error: Throwable) {
        handler.post {
            // Kept for skipFailed(), which may run later if this entry is not current yet.
            failures[token] = error
            val entry = queue.find { it.token == token } ?: return@post
            // A failed preload is reported when the receiver reaches the entry.
            if (entry === (pending?.entry ?: receiverEntry() ?: queue.getOrNull(lastIndex)))
                skipFailed(entry)
        }
    }

    // Media3's CastPlayer uses this for the entries the receiver reports. What we expose is
    // our own queue.
    private inner class ItemConverter : MediaItemConverter {
        override fun toMediaQueueItem(mediaItem: MediaItem) = newEntry(mediaItem).toQueueItem()
        override fun toMediaItem(mediaQueueItem: MediaQueueItem): MediaItem {
            val contentId = mediaQueueItem.media?.contentId
            val token = server.tokenOf(contentId)
            // An id we do not know gets a bare item, only the media id survives.
            return queue.find { it.token == token }?.item
                ?: MediaItem.Builder().setMediaId(contentId ?: "").build()
        }
    }

    // Keeps the phone awake and on Wi-Fi while the receiver streams from it.
    private class NetworkLocks(context: Context) {
        private val wakeLock = (context.getSystemService(Context.POWER_SERVICE) as PowerManager)
            .newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "Echo:Cast")
            .apply { setReferenceCounted(false) }

        // High performance mode stops the Wi-Fi radio dozing while the screen is off.
        @Suppress("DEPRECATION")
        private val wifiLock =
            (context.applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager)
                .createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "Echo:Cast")
                .apply { setReferenceCounted(false) }

        fun setHeld(held: Boolean) {
            if (held) {
                if (!wakeLock.isHeld) wakeLock.acquire()
                if (!wifiLock.isHeld) wifiLock.acquire()
            } else {
                if (wakeLock.isHeld) wakeLock.release()
                if (wifiLock.isHeld) wifiLock.release()
            }
        }
    }

    companion object {
        private const val MAX_ITEMS_PER_MESSAGE = 50
        private const val PRELOAD_TIME_SECONDS = 20.0
        private const val PENDING_TIMEOUT_MS = 10_000L
        private const val OP_TIMEOUT_MS = 5_000L
        private const val RESYNC_INTERVAL_MS = 2_000L

        private val REMOTE_COMMANDS = intArrayOf(
            Player.COMMAND_SET_SHUFFLE_MODE,
            Player.COMMAND_SEEK_IN_CURRENT_MEDIA_ITEM,
            Player.COMMAND_SEEK_TO_PREVIOUS_MEDIA_ITEM,
            Player.COMMAND_SEEK_TO_PREVIOUS,
            Player.COMMAND_SEEK_TO_NEXT_MEDIA_ITEM,
            Player.COMMAND_SEEK_TO_NEXT,
            Player.COMMAND_SEEK_BACK,
            Player.COMMAND_SEEK_FORWARD,
        )

        fun isAvailable(context: Context) = runCatching {
            GoogleApiAvailability.getInstance()
                .isGooglePlayServicesAvailable(context) == ConnectionResult.SUCCESS
        }.getOrDefault(false)

        fun castContext(context: Context): CastContext? {
            if (!isAvailable(context)) return null
            @Suppress("DEPRECATION")
            return runCatching { CastContext.getSharedInstance(context) }.getOrNull()
        }

        /** Returns null when Google Play services, and so Cast, is not available. */
        fun create(
            context: Context,
            localPlayer: Player,
            app: App,
            scope: CoroutineScope,
            state: PlayerState,
            extensions: ExtensionLoader,
            downloadFlow: StateFlow<List<Downloader.Info>>,
            mediaChangeFlow: MutableSharedFlow<Pair<MediaItem, MediaItem>>,
        ): CastSwitchPlayer? {
            val castContext = castContext(context) ?: return null
            return CastSwitchPlayer(
                context, localPlayer, castContext, app, scope,
                state, extensions, downloadFlow, mediaChangeFlow
            )
        }

        // The address of this device that the receiver can reach. Connecting a UDP socket sends
        // nothing, it only asks the system which interface it would route through.
        private fun localAddressFor(remote: InetAddress?): InetAddress? {
            val routed = remote?.let {
                runCatching {
                    DatagramSocket().use { socket ->
                        // 8009 is the Cast receiver's port, any port would route the same way.
                        socket.connect(it, 8009)
                        socket.localAddress
                    }
                }.getOrNull()
            }
            val isUsable = routed != null && !routed.isAnyLocalAddress && !routed.isLoopbackAddress
            if (isUsable) return routed
            // Routing gave nothing usable, so take the first LAN IPv4 address of an active interface.
            return NetworkInterface.getNetworkInterfaces().toList()
                .filter { it.isUp && !it.isLoopback }
                .flatMap { it.inetAddresses.toList() }
                .firstOrNull { it is Inet4Address && it.isSiteLocalAddress }
        }
    }
}
