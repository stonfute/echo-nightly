package dev.brahmkshatriya.echo.playback.cast

import android.content.Context
import android.net.wifi.WifiManager
import android.os.Handler
import android.os.PowerManager
import android.os.SystemClock
import android.util.Log
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
import com.google.android.gms.cast.CastStatusCodes
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
import com.google.android.gms.common.api.Status
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

    private sealed class Op {
        // How many times the receiver has rejected this change.
        var attempts = 0
    }

    private class Load(val entries: List<Entry>, val positionMs: Long, val autoplay: Boolean) : Op()
    private class Insert(val entries: List<Entry>) : Op()
    private class Remove(val entries: List<Entry>) : Op()
    private class Move(val entries: List<Entry>) : Op()
    private class Jump(val entry: Entry, val positionMs: Long) : Op()

    // Class names do not survive R8, so the logs get these descriptions instead.
    private fun Op.describe() = when (this) {
        is Load -> "load of ${entries.size} tracks"
        is Insert -> "insert of ${entries.size} tracks"
        is Remove -> "removal of ${entries.size} tracks"
        is Move -> "move of ${entries.size} tracks"
        is Jump -> "jump"
    }

    // Changes still to send, in order. Most wait until the receiver has reported the ids of the
    // entries they touch.
    private val ops = ArrayDeque<Op>()
    // The op the watchdog is currently timing.
    private var watchedOp: Op? = null
    // Loads sent and not answered. Changes wait for this to reach 0, because the receiver
    // rejects them before it has the queue.
    private var loadsInFlight = 0

    // Queue changes sent and not answered yet. The Cast SDK only follows the last request of a
    // kind and answers earlier ones with REPLACED, even though the receiver applied them. So we
    // send one change at a time.
    private var changesInFlight = 0
    // Used to rate-limit resync().
    private var lastResync = 0L

    // Rebuilds around the current track since the queue was last in sync. After
    // MAX_SOFT_RESYNCS we reload, which restarts the track.
    private var softResyncs = 0
    // Last receiver state we logged, so the log only shows changes.
    private var lastReceiverState: String? = null

    // Receiver ids removed by a rebuild. The id list is not trusted while it still has any.
    private val staleIds = mutableSetOf<Int>()

    // Bumped whenever the receiver's queue is replaced, so a late answer to an older change is
    // ignored instead of retried on the new queue.
    private var generation = 0

    // The entry whose successor we last resolved ahead of the receiver.
    private var prefetchedAfter: Entry? = null

    private val sessionListener = object : SessionAvailabilityListener {
        override fun onCastSessionAvailable() = connect()
        override fun onCastSessionUnavailable() = disconnect()
    }

    private val castListener = object : Player.Listener {
        override fun onEvents(player: Player, events: Player.Events) {
            // The CastPlayer stays registered while we play on the phone, so skip its events then.
            if (!isRemote) return
            // Only keep the phone awake while the receiver is meant to be playing.
            locks.setHeld(castPlayer.playWhenReady)
            logReceiverState()
            prefetchNext()
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

    // If the receiver never reports the ids an op waits for, start again from our queue. A load
    // can be slow, since the receiver waits for the extension to resolve the stream.
    private val watchdog = object : Runnable {
        override fun run() {
            // A request is still outstanding, so the ids may just be late.
            if (loadsInFlight > 0 || changesInFlight > 0) {
                handler.postDelayed(this, OP_TIMEOUT_MS)
                return
            }
            Log.w(TAG, "The receiver has not reported what a ${ops.firstOrNull()?.describe()} waits for")
            resync()
        }
    }
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
        Log.i(TAG, "Moving playback to the Cast device: ${items.size} tracks, index $index at ${position}ms")

        queue.clear()
        items.mapTo(queue) { newEntry(it) }
        lastIndex = index
        // Start from a clean slate, nothing from an earlier session applies.
        changesInFlight = 0
        failures.clear()
        skippedTokens.clear()
        // Listen to the receiver's queue so we hear when its item ids change.
        registeredQueue = remoteClient?.mediaQueue?.also { it.registerCallback(queueCallback) }
        // load() sets a pending target, so the UI shows the track while the receiver starts.
        if (queue.isNotEmpty()) load(index, position, playWhenReady)

        setPlayer(castPlayer)
        // Stopped after the switch, so the listeners never see the phone's player go idle.
        localPlayer.stop()
        locks.setHeld(playWhenReady)
    }

    private fun disconnect() {
        connectJob?.cancel()
        // We never got to the remote player, only the server needs to go.
        if (!isRemote) return server.stop()
        val entries = queue.toList()
        val index = currentMediaItemIndex
        val position = currentPosition
        Log.i(TAG, "Cast session ended, moving playback back to the phone at index $index")

        // Drop all receiver bookkeeping. The generation bump makes late answers harmless.
        ops.clear()
        pending = null
        loadsInFlight = 0
        changesInFlight = 0
        softResyncs = 0
        lastReceiverState = null
        prefetchedAfter = null
        staleIds.clear()
        generation++
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

    // The receiver reports a content URL, the token inside it maps back to our entry.
    private fun receiverToken() = server.tokenOf(remoteClient?.mediaStatus?.mediaInfo?.contentId)
    private fun receiverEntry() = receiverToken()?.let { token -> queue.find { it.token == token } }

    // Commands

    override fun handleSetPlayWhenReady(playWhenReady: Boolean): ListenableFuture<*> {
        // No media on the receiver means resuming has to load our queue again.
        if (isRemote && playWhenReady && isReceiverIdle()) {
            Log.i(TAG, "Play while the receiver is idle, sending the queue again")
            reload(true)
        }
        return super.handleSetPlayWhenReady(playWhenReady)
    }

    override fun handlePrepare(): ListenableFuture<*> {
        if (!isRemote) return super.handlePrepare()
        if (isReceiverIdle()) {
            Log.i(TAG, "Prepare while the receiver is idle, sending the queue again")
            reload(playWhenReady)
        }
        return Futures.immediateVoidFuture()
    }

    override fun handleStop(): ListenableFuture<*> {
        if (!isRemote) return super.handleStop()
        // After a stop the receiver's ids mean nothing, the next play sends the queue again.
        ops.clear()
        generation++
        staleIds.clear()
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
            // Nothing loaded on the receiver, so even a seek has to load the queue.
            isReceiverIdle() -> load(mediaItemIndex, position, playWhenReady)
            // Same track as the receiver is on, a plain seek is enough.
            pending == null && entry === receiverEntry() -> {
                Log.i(TAG, "Seek to ${position}ms")
                castPlayer.seekTo(position)
            }

            // A different track: show it as the target and jump once its item id is known.
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

    // Replaces the receiver's queue with ours. A Cast message is capped at 64KB, so big queues
    // go out in several messages.
    private fun load(index: Int, positionMs: Long, autoplay: Boolean) {
        if (queue.isEmpty()) return
        // Everything queued before is obsolete once the queue is replaced.
        ops.clear()
        generation++
        staleIds.clear()
        // New tokens and no ids, so old reports from the receiver cannot match these entries.
        queue.forEach {
            it.token = server.reissue(it.token, it.item)
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

    // For when we can no longer follow the receiver's queue. We first rebuild it around the
    // playing track, which does not interrupt it. A full reload restarts that track, so it is
    // the last resort.
    private fun resync() {
        if (!isRemote || queue.isEmpty()) return
        if (loadsInFlight == 0 && remoteClient?.mediaStatus?.mediaInfo == null) {
            // Nothing loaded, the next prepare or play sends the queue.
            ops.clear()
            return
        }
        val now = SystemClock.elapsedRealtime()
        // Too soon after the last one: look again later rather than loop on a flaky receiver.
        if (now - lastResync < RESYNC_INTERVAL_MS) {
            handler.removeCallbacks(watchdog)
            handler.postDelayed(watchdog, RESYNC_INTERVAL_MS)
            return
        }
        lastResync = now
        val client = remoteClient
        if (client != null && softResyncs < MAX_SOFT_RESYNCS && rebuildAroundCurrent(client)) {
            softResyncs++
            return
        }
        // Rebuilding did not hold or is not possible, so reload and accept the restart.
        softResyncs = 0
        Log.w(TAG, "Receiver queue out of sync, reloading it")
        load(currentMediaItemIndex, currentPosition, playWhenReady)
    }

    // Swaps every receiver item except the playing one for our queue, without touching playback.
    private fun rebuildAroundCurrent(client: RemoteMediaClient): Boolean {
        // We need a settled receiver and a playing item we recognise, or there is no anchor.
        if (loadsInFlight > 0 || pending != null) return false
        val currentId = client.mediaStatus?.currentItemId ?: return false
        if (currentId == MediaQueueItem.INVALID_ITEM_ID) return false
        val current = receiverEntry() ?: return false
        val index = queue.indexOf(current)
        if (index < 0) return false
        // Everything the receiver has except the playing item is to be removed.
        val stale = client.mediaQueue.itemIds.filter { it != currentId }.toIntArray()
        Log.w(
            TAG, "Receiver queue out of sync, rebuilding it around the current track " +
                    "(${stale.size} receiver items replaced by ${queue.size - 1})"
        )
        ops.clear()
        generation++
        // Until the receiver stops listing these, its id list cannot be trusted.
        staleIds.addAll(stale.toList())
        queue.forEach {
            if (it === current) {
                it.sent = true
                it.itemId = currentId
            } else {
                // New tokens, so whatever the receiver still reports for its old items is ignored.
                it.token = server.reissue(it.token, it.item)
                it.itemId = MediaQueueItem.INVALID_ITEM_ID
                it.sent = false
            }
        }
        if (stale.isNotEmpty()) {
            // Counted as a change in flight, so the inserts below wait for the removal.
            changesInFlight++
            client.queueRemoveItems(stale, null).setResultCallback {
                changesInFlight = (changesInFlight - 1).coerceAtLeast(0)
                if (!isRemote) return@setResultCallback
                if (it.status.isSuccess) return@setResultCallback flushOps()
                // Their tokens are gone, so the receiver would fail on them. Rebuild again or reload.
                Log.w(TAG, "Could not remove old receiver items: ${it.status}")
                resync()
            }
        }
        // The playing item stays: what follows goes in behind it, what precedes it goes in front.
        val after = queue.subList(index + 1, queue.size).toList()
        val before = queue.subList(0, index).toList()
        if (after.isNotEmpty()) ops.addLast(Insert(after))
        if (before.isNotEmpty()) ops.addLast(Insert(before))
        flushOps()
        return true
    }

    // The receiver rejects queue changes until it has loaded the queue they apply to, and we only
    // have one change in the air at a time.
    private fun isReceiverReady(client: RemoteMediaClient) =
        loadsInFlight == 0 && changesInFlight == 0 && client.mediaStatus?.mediaInfo != null

    private fun execute(client: RemoteMediaClient, op: Op): Boolean = when {
        // A load replaces the queue, so it never has to wait for the receiver to be ready.
        op is Load -> {
            executeLoad(client, op)
            true
        }

        !isReceiverReady(client) -> false
        else -> executeChange(client, op)
    }

    private fun executeLoad(client: RemoteMediaClient, op: Load) {
        val queueData = MediaQueueData.Builder()
            .setItems(op.entries.map { it.toQueueItem() })
            // The entries already begin at the track to play, see load().
            .setStartIndex(0)
            .setRepeatMode(castRepeatMode())
            .build()
        val request = MediaLoadRequestData.Builder()
            .setQueueData(queueData)
            .setAutoplay(op.autoplay)
            .setCurrentTime(op.positionMs)
            .build()
        loadsInFlight++
        Log.i(
            TAG, "Loading ${op.entries.size} tracks on the receiver at ${op.positionMs}ms, " +
                    "autoplay ${op.autoplay}"
        )
        client.load(request).setResultCallback {
            loadsInFlight = (loadsInFlight - 1).coerceAtLeast(0)
            if (!it.status.isSuccess) {
                // Tell the user, they would otherwise just see a silent receiver.
                Log.w(TAG, "Cast load failed: ${it.status}")
                scope.launch {
                    app.throwFlow.emit(Exception("Cast device could not load the queue"))
                }
            }
            flushOps()
            invalidateIfRemote()
        }
        // Marked sent right away, their ids show up later through reconcile().
        op.entries.forEach { it.sent = true }
    }

    private fun executeChange(client: RemoteMediaClient, op: Op): Boolean = when (op) {
        is Load -> throw IllegalArgumentException("Loads are executed by executeLoad")

        is Insert -> {
            val inserted = op.entries.toSet()
            val entries = queue.filter { it in inserted && !it.sent }
            // Insert in front of the next entry the receiver already has. No such entry means
            // the end of its queue.
            val before = entries.lastOrNull()?.let { receiverEntryAfter(it, inserted) }
            when {
                entries.isEmpty() -> true
                // That entry's id is not known yet, so we cannot say where to insert.
                before != null && !before.hasId -> false
                else -> {
                    // One message at a time. The rest goes out once the receiver has answered,
                    // before the same item, so it lands after this part.
                    val beforeId = before?.itemId ?: MediaQueueItem.INVALID_ITEM_ID
                    val chunk = entries.take(MAX_ITEMS_PER_MESSAGE)
                    Log.i(
                        TAG, "Inserting ${chunk.size} of ${entries.size} tracks " +
                                "before receiver item $beforeId"
                    )
                    val items = chunk.map { it.toQueueItem() }.toTypedArray()
                    // A retry only has to cover this chunk, and keeps the attempts used so far.
                    val retry = Insert(chunk).also { it.attempts = op.attempts }
                    client.queueInsertItems(items, beforeId, null).trackChange(retry)
                    // Marked sent now so the next pass does not insert them twice.
                    chunk.forEach { it.sent = true }
                    chunk.size == entries.size
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
                    Log.i(TAG, "Removing receiver items ${ids.joinToString()}")
                    client.queueRemoveItems(ids, null).trackChange(op)
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
                    Log.i(TAG, "Moving receiver items ${ids.joinToString()} before $beforeId")
                    client.queueReorderItems(ids, beforeId, null).trackChange(op)
                    true
                }
            }
        }

        is Jump -> when {
            // The target was removed meanwhile, drop the jump.
            op.entry !in queue -> true
            !op.entry.sent || !op.entry.hasId -> false
            else -> {
                Log.i(TAG, "Jumping to receiver item ${op.entry.itemId} at ${op.positionMs}ms")
                client.queueJumpToItem(op.entry.itemId, op.positionMs, null).trackChange(op)
                true
            }
        }
    }

    // Counts [op] as in flight until the receiver answers, then sends the next change.
    private fun PendingResult<RemoteMediaClient.MediaChannelResult>.trackChange(op: Op) {
        // Which queue this was sent to, so an answer after a rebuild or load can be ignored.
        val sentIn = generation
        changesInFlight++
        setResultCallback { result ->
            changesInFlight = (changesInFlight - 1).coerceAtLeast(0)
            if (!isRemote) return@setResultCallback
            val status = result.status
            when {
                // The receiver's queue was replaced since, so this answer no longer matters.
                sentIn != generation || status.isSuccess -> Unit
                // The SDK stopped following it, the receiver did not refuse it. Never resend
                // these, that is what filled the receiver with duplicates. The id list shows
                // what really happened.
                status.statusCode == CastStatusCodes.REPLACED ->
                    Log.w(TAG, "Lost track of a ${op.describe()}, its result is unknown")

                else -> return@setResultCallback retry(op, status)
            }
            flushOps()
        }
    }

    // A rejected change is tried a few times before we rebuild the receiver's queue.
    private fun retry(op: Op, status: Status) {
        Log.w(TAG, "The receiver rejected a ${op.describe()}: $status")
        // The user already picked another track, so this jump is stale.
        if (op is Jump && pending?.entry !== op.entry) return flushOps()
        op.attempts++
        // It keeps failing, so our picture of the receiver's queue is probably wrong.
        if (op.attempts >= MAX_ATTEMPTS) return resync()
        when (op) {
            // Forget the ids too, the insert starts over from scratch.
            is Insert -> op.entries.forEach {
                it.sent = false
                it.itemId = MediaQueueItem.INVALID_ITEM_ID
            }

            is Remove -> op.entries.forEach { if (it.hasId) it.sent = true }
            else -> Unit
        }
        // Back to the head so order is kept, and a short delay so we do not hammer the receiver.
        ops.addFirst(op)
        handler.removeCallbacks(flushRunnable)
        handler.postDelayed(flushRunnable, RETRY_DELAY_MS)
    }

    // The next entry after [entry] that the receiver already has, skipping those in [except].
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
        // The receiver lists its item ids in queue order. That list can lag behind a change, so
        // we only trust it when it agrees with the ids matched by content id above.
        val ids = client.mediaQueue.itemIds
        if (staleIds.isNotEmpty()) {
            // Right after a rebuild the list can still hold the removed items.
            if (ids.any { it in staleIds }) return
            staleIds.clear()
        }
        val sent = queue.filter { it.sent }
        // A different count means the list lags. With no id matched yet there is nothing to
        // check it against.
        if (ids.size != sent.size || sent.none { it.hasId }) return
        // An id in the wrong position means the order differs, so do not copy any of it.
        if (sent.indices.any { sent[it].hasId && sent[it].itemId != ids[it] }) return
        sent.forEachIndexed { i, entry -> entry.itemId = ids[i] }
        // Only a queue that stayed in sync for a while counts as fixed by the last rebuild.
        if (SystemClock.elapsedRealtime() - lastResync > STABLE_SYNC_MS) softResyncs = 0
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
            albumArtist?.let { metadata.putString(MediaMetadata.KEY_ALBUM_ARTIST, it.toString()) }
        }
        server.artUri(token, item)?.let { metadata.addImage(WebImage(it.toUri())) }
        val url = server.trackUrl(token)
        val info = MediaInfo.Builder(url)
            .setContentUrl(url)
            .setStreamType(MediaInfo.STREAM_TYPE_BUFFERED)
            .setContentType(CastStreamServer.DEFAULT_TYPE)
            .setMetadata(metadata)
            .build()
        return MediaQueueItem.Builder(info)
            // Start as soon as it becomes the current item.
            .setAutoplay(true)
            // Asks the receiver to start fetching this many seconds early. The Default Media
            // Receiver didn't do it for plain files in my tests, hence prefetchNext().
            .setPreloadTime(PRELOAD_TIME_SECONDS)
            .build()
    }

    // Errors

    // Logs the receiver's state when it changes. Buffering mid-track means it ran out of data,
    // which points at our stream server or the network.
    private fun logReceiverState() {
        val status = remoteClient?.mediaStatus
        val state = when (status?.playerState) {
            null -> "has no media"
            MediaStatus.PLAYER_STATE_PLAYING -> "playing"
            MediaStatus.PLAYER_STATE_PAUSED -> "paused"
            MediaStatus.PLAYER_STATE_BUFFERING -> "buffering"
            MediaStatus.PLAYER_STATE_IDLE -> "idle (reason ${status.idleReason})"
            else -> "in state ${status.playerState}"
        }
        val described = "$state, item ${status?.currentItemId}"
        if (described == lastReceiverState) return
        lastReceiverState = described
        Log.i(TAG, "Receiver $described at ${status?.streamPosition}ms")
    }

    // The receiver only asks for the next track once the current one ends, then waits while the
    // extension resolves it. So we resolve it as soon as the current one starts.
    private fun prefetchNext() {
        // While a jump is pending we do not know yet which track will play.
        if (pending != null) return
        val current = receiverEntry() ?: return
        // Once per track, this runs on every player event.
        if (current === prefetchedAfter) return
        prefetchedAfter = current
        val index = queue.indexOf(current)
        val next = when {
            // Repeat one replays the same track, nothing to resolve.
            index < 0 || castPlayer.repeatMode == Player.REPEAT_MODE_ONE -> null
            index < queue.lastIndex -> queue[index + 1]
            // The last track wraps around to the first.
            castPlayer.repeatMode == Player.REPEAT_MODE_ALL -> queue.first()
            else -> null
        } ?: return
        server.prefetch(next.token)
    }

    private fun checkReceiverError() {
        // An error on the old track says nothing about the one we are moving to.
        if (pending != null) return
        val status = remoteClient?.mediaStatus ?: return
        if (status.playerState != MediaStatus.PLAYER_STATE_IDLE) return
        // Idle because of an error means the receiver gave up on this track.
        if (status.idleReason != MediaStatus.IDLE_REASON_ERROR) return
        skipFailed(receiverEntry() ?: return)
    }

    // Reports why the current entry failed and moves on to the next one, once per entry.
    private fun skipFailed(entry: Entry) {
        if (!skippedTokens.add(entry.token)) return
        val error = failures[entry.token] ?: Exception("The Cast device could not play this track")
        Log.w(TAG, "Skipping a track the receiver could not play", error)
        scope.launch { app.throwFlow.emit(PlayerException(entry.item, error)) }
        val index = queue.indexOf(entry)
        // The last track has no successor to skip to.
        if (index in 0 until queue.lastIndex) seekTo(index + 1, 0)
    }

    override fun onLoaded(token: String, old: MediaItem, new: MediaItem) {
        // Callback comes from the server, queue changes belong on the player's thread.
        handler.post {
            // Fall back to the old item in case the token was reissued in the meantime.
            val entry = queue.find { it.token == token || it.item === old } ?: return@post
            entry.item = new
            server.update(entry.token, new)
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
        // Keeps one Cast message under its 64KB limit.
        private const val MAX_ITEMS_PER_MESSAGE = 50
        private const val PRELOAD_TIME_SECONDS = 20.0
        // How long the UI shows a pending target before trusting the receiver again.
        private const val PENDING_TIMEOUT_MS = 10_000L
        // How long an op may wait for ids before the watchdog steps in.
        private const val OP_TIMEOUT_MS = 10_000L
        private const val RESYNC_INTERVAL_MS = 10_000L
        private const val RETRY_DELAY_MS = 1_000L
        // Rejections of one change before we give up on it and resync.
        private const val MAX_ATTEMPTS = 3
        private const val MAX_SOFT_RESYNCS = 2
        // A minute in sync resets the soft resync count.
        private const val STABLE_SYNC_MS = 60_000L
        private const val TAG = "CastSwitchPlayer"

        // Added on top of what the CastPlayer reports, since we handle these against our queue.
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
