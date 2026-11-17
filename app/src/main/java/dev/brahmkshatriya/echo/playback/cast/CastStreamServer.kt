package dev.brahmkshatriya.echo.playback.cast

import android.net.Uri
import android.os.SystemClock
import android.util.Log
import android.webkit.MimeTypeMap
import androidx.core.net.toUri
import androidx.media3.common.MediaItem
import dev.brahmkshatriya.echo.common.models.ImageHolder
import dev.brahmkshatriya.echo.common.models.NetworkRequest
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.di.App
import dev.brahmkshatriya.echo.download.Downloader
import dev.brahmkshatriya.echo.extensions.ExtensionLoader
import dev.brahmkshatriya.echo.playback.MediaItemUtils
import dev.brahmkshatriya.echo.playback.MediaItemUtils.extensionId
import dev.brahmkshatriya.echo.playback.MediaItemUtils.sourceIndex
import dev.brahmkshatriya.echo.playback.MediaItemUtils.track
import dev.brahmkshatriya.echo.playback.PlayerService.Companion.select
import dev.brahmkshatriya.echo.playback.PlayerState
import dev.brahmkshatriya.echo.playback.source.StreamableLoader
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.flow.StateFlow
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.FilterOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * Small HTTP server on the phone that a Cast receiver pulls tracks from.
 *
 * The receiver downloads media itself. It cannot send the headers extensions need, read
 * [Streamable.Source.Raw] streams or open files on the phone. So each queued track gets a random
 * token and the receiver is given a URL here. The track is resolved through [StreamableLoader] on
 * first request, then proxied with Range support so seeking works.
 */
class CastStreamServer(
    private val app: App,
    private val state: PlayerState,
    extensions: ExtensionLoader,
    downloadFlow: StateFlow<List<Downloader.Info>>,
    private val listener: Listener,
) {

    interface Listener {
        /** Background thread. [old] was resolved and [new] is the item to use from now on. */
        fun onLoaded(token: String, old: MediaItem, new: MediaItem)

        /** Background thread. The track behind [token] could not be resolved or cannot be cast. */
        fun onFailed(token: String, item: MediaItem, error: Throwable)
    }

    // TODO: HLS, DASH and DRM sources cannot be cast, this server only relays plain byte streams.
    class UnsupportedSourceException(message: String) : Exception(message)

    private class Loaded(val item: MediaItem, val source: Streamable.Source)

    private val loader = StreamableLoader(app, extensions.music, downloadFlow)
    private val client = OkHttpClient.Builder().build()
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Token to the queued item. Swapped for the resolved item once the track has loaded.
    private val items = ConcurrentHashMap<String, MediaItem>()

    // One resolution per token, shared by prefetch() and the receiver's own request.
    private val loads = ConcurrentHashMap<String, Deferred<Loaded>>()

    // Type sniffed from the first bytes of a Raw track. Later Range requests start mid-file and
    // cannot sniff it again.
    private val types = ConcurrentHashMap<String, String>()

    @Volatile
    private var serverSocket: ServerSocket? = null

    @Volatile
    private var executor: ExecutorService? = null

    @Volatile
    private var baseUrl: String? = null

    fun register(item: MediaItem): String {
        val token = UUID.randomUUID().toString().replace("-", "")
        items[token] = item
        return token
    }

    fun update(token: String, item: MediaItem) {
        items.computeIfPresent(token) { _, _ -> item }
    }

    /**
     * Gives the track a new token and carries its resolved stream over, so rebuilding the
     * receiver's queue does not hit the extension a second time.
     */
    fun reissue(token: String, item: MediaItem): String {
        val new = register(items.remove(token) ?: item)
        loads.remove(token)?.let { loads[new] = it }
        types.remove(token)?.let { types[new] = it }
        return new
    }

    fun clear() {
        items.clear()
        types.clear()
        loads.values.forEach { it.cancel() }
        loads.clear()
    }

    /**
     * Resolves the track early. The receiver only asks for the next track once the current one
     * has ended, so without this there is a gap while the extension answers.
     */
    fun prefetch(token: String) {
        if (items.containsKey(token) && !loads.containsKey(token)) {
            Log.i(TAG, "Resolving track ${token.take(8)} ahead of the receiver")
            load(token)
        }
    }

    fun trackUrl(token: String) = "${requireNotNull(baseUrl)}/$TRACK/$token"
    fun artUrl(token: String) = "${requireNotNull(baseUrl)}/$ART/$token"

    fun tokenOf(url: String?): String? {
        // The token is the last path segment of both /track/ and /art/ URLs.
        val token = url?.substringAfterLast('/') ?: return null
        return token.takeIf { items.containsKey(it) }
    }

    /**
     * Cover URL for the receiver. Plain public HTTPS covers go out directly, since receivers and
     * smart displays load those themselves and may refuse images from a local address. Anything
     * that needs headers or lives on the phone is served by this server.
     */
    fun artUri(token: String, item: MediaItem): String? =
        when (val cover = runCatching { item.track.cover }.getOrNull()) {
            is ImageHolder.NetworkRequestImageHolder -> {
                val request = cover.request
                if (request.isPlainHttps()) request.url else artUrl(token)
            }

            is ImageHolder.ResourceUriImageHolder -> artUrl(token)
            else -> null
        }

    private fun NetworkRequest.isPlainHttps() = method == NetworkRequest.Method.GET &&
            headers.isEmpty() && bodyBase64 == null && url.startsWith("https://", true)

    /**
     * Listens on [host], the phone's address on the Wi-Fi the receiver is on. Binding to that
     * address alone keeps the server off mobile data.
     */
    fun start(host: InetAddress) {
        stop()
        // Port 0 takes any free port, 50 is the backlog of pending connections.
        val socket = ServerSocket(0, 50, host)
        val pool = Executors.newCachedThreadPool {
            Thread(it, "CastStreamServer").apply { isDaemon = true }
        }
        serverSocket = socket
        executor = pool
        // Drop the IPv6 scope id (fe80::1%wlan0), it is not valid inside a URL.
        val hostName = host.hostAddress!!.substringBefore('%')
        // IPv6 literals need square brackets in a URL.
        val urlHost = if (hostName.contains(':')) "[$hostName]" else hostName
        baseUrl = "http://$urlHost:${socket.localPort}"
        Log.i(TAG, "Serving Cast streams on $baseUrl")
        pool.execute {
            while (!socket.isClosed) {
                // accept() throws once stop() closes the socket, which ends the loop.
                val connection = runCatching { socket.accept() }.getOrNull() ?: break
                // One thread per connection. If the pool is already shut down, drop the socket.
                runCatching { pool.execute { handle(connection) } }
                    .onFailure { connection.close() }
            }
        }
    }

    fun stop() {
        serverSocket?.close()
        serverSocket = null
        executor?.shutdownNow()
        executor = null
        baseUrl = null
        clear()
    }

    fun release() {
        stop()
        scope.cancel()
    }

    private fun load(token: String) = loads.computeIfAbsent(token) {
        scope.async {
            val item = items[token] ?: throw NoSuchElementException("Unknown track")
            // Same loader as local playback, so the receiver gets the stream the phone would play.
            runCatching {
                val (loaded, server) = loader.load(item)
                state.servers[loaded.mediaId] = server
                state.serverChanged.emit(Unit)
                val (source, new) = selectSource(loaded, server.getOrThrow())
                // Only if the token is still registered, clear() may have run meanwhile.
                items.computeIfPresent(token) { _, _ -> new }
                listener.onLoaded(token, item, new)
                Loaded(new, source)
            }.getOrElse {
                // Cancellation comes from clear() or stop(), nobody needs to hear about it.
                if (it !is CancellationException) {
                    // Drop the failed load so the receiver's own request can try again, for
                    // example after a network hiccup during a prefetch.
                    loads.remove(token)
                    listener.onFailed(token, item, it)
                }
                // Rethrown so await() in the request handler sees the same error.
                throw it
            }
        }
    }

    // Picks a source the way StreamableMediaSource does, minus the ones this server cannot relay.
    private fun selectSource(
        item: MediaItem, server: Streamable.Media.Server,
    ): Pair<Streamable.Source, MediaItem> {
        val sources = server.sources
        if (sources.isEmpty()) throw Exception("No sources found")
        val castable = sources.filter { it.isCastable() }
        if (castable.isEmpty()) throw UnsupportedSourceException(
            "Casting HLS, DASH or DRM protected streams is not supported"
        )
        // A lone source leaves nothing to choose.
        if (sources.size == 1) return castable.first() to item
        val candidates = if (server.merged) castable.filter { !it.isVideo }.ifEmpty { castable }
        else castable
        // Keep the source the item already points at, otherwise choose by quality.
        val source = sources.getOrNull(item.sourceIndex)?.takeIf { it in candidates }
            ?: candidates.select(app, item.extensionId) { it.quality }
        if (server.merged) return source to item
        // Store the chosen index in the item so later loads pick the same source.
        return source to MediaItemUtils.buildSource(item, sources.indexOf(source))
    }

    // The server relays plain bytes, so only Raw providers and unencrypted progressive HTTP fit.
    private fun Streamable.Source.isCastable() = when (this) {
        is Streamable.Source.Raw -> streamProvider != null
        is Streamable.Source.Http ->
            type == Streamable.SourceType.Progressive && decryption == null
    }

    private class Range(val start: Long, val end: Long?)

    private class HttpRequest(
        val method: String, val path: String, val range: Range?, val rawRange: String?,
    )

    /** Marks a failure on the source side, so the log can tell it from the receiver hanging up. */
    private class SourceException(message: String, cause: Throwable? = null) :
        IOException(message, cause)

    // Wraps the socket output to count body bytes for the request log. Bytes written before the
    // head went out are not counted.
    private class ResponseStream(out: OutputStream) : FilterOutputStream(out) {
        var code = 0
            private set
        var declaredLength = -1L
            private set
        var bodyBytes = 0L
            private set
        var headSentAt = 0L
            private set
        private var headSent = false

        fun onHeadSent(code: Int, declaredLength: Long) {
            this.code = code
            this.declaredLength = declaredLength
            headSentAt = SystemClock.elapsedRealtime()
            headSent = true
        }

        override fun write(b: Int) {
            out.write(b)
            if (headSent) bodyBytes++
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            out.write(b, off, len)
            if (headSent) bodyBytes += len
        }
    }

    // Only used for the request log.
    private val openConnections = AtomicInteger()

    private fun handle(socket: Socket) = socket.use {
        val startedAt = SystemClock.elapsedRealtime()
        val connections = openConnections.incrementAndGet()
        var request: HttpRequest? = null
        var output: ResponseStream? = null
        // Failures are caught here and logged with the request, a worker thread never throws.
        val error = runCatching {
            // A silent client gives up its thread after 30 seconds of no data.
            socket.soTimeout = 30_000
            val input = BufferedInputStream(socket.getInputStream())
            val response =
                ResponseStream(BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE))
            output = response
            // Empty or malformed request line: nothing to answer.
            val parsed = readRequest(input) ?: return@runCatching
            request = parsed
            respond(parsed, response)
            response.flush()
        }.exceptionOrNull()
        openConnections.decrementAndGet()
        request?.let { log(it, output, startedAt, connections, error) }
    }

    // One line per request, so logcat shows whether the receiver or the source misbehaved.
    private fun log(
        request: HttpRequest,
        output: ResponseStream?,
        startedAt: Long,
        connections: Int,
        error: Throwable?,
    ) {
        val ms = SystemClock.elapsedRealtime() - startedAt
        // How long the receiver waited for the answer to start, mostly the track being resolved.
        val waited = output?.headSentAt?.takeIf { it > 0 }?.let { "answered after ${it - startedAt}ms, " }
            ?: ""
        val path = request.path.substringBefore('?').trim('/')
        // Kind plus the first 8 characters of the token, enough to match lines to a track.
        val name = "${path.substringBefore('/')}/${path.substringAfter('/', "").take(8)}"
        val code = output?.code?.takeIf { it != 0 }
        val sent = output?.run {
            if (declaredLength >= 0) "$bodyBytes/$declaredLength" else "$bodyBytes"
        } ?: "0"
        val message = "${request.method} $name range=${request.rawRange ?: "none"} -> " +
                "${code ?: "no response"}, ${waited}sent $sent bytes in ${ms}ms, $connections open"
        when {
            // Cover requests log at debug to keep the info level readable.
            error == null -> if (name.startsWith(ART)) Log.d(TAG, message) else Log.i(TAG, message)
            error is SourceException -> Log.w(TAG, "$message, the source failed", error)
            // An IOException after the head went out is a write to the receiver failing, which
            // is what a seek or a stop looks like.
            error is IOException && code != null ->
                Log.i(TAG, "$message, closed by the receiver: $error")

            else -> Log.w(TAG, "$message, failed", error)
        }
    }

    private fun readRequest(input: InputStream): HttpRequest? {
        val requestLine = input.readHttpLine() ?: return null
        val parts = requestLine.split(' ')
        if (parts.size < 2) return null
        var rawRange: String? = null
        // Only Range matters, every other header is read and dropped.
        while (true) {
            val line = input.readHttpLine() ?: break
            // An empty line ends the headers.
            if (line.isEmpty()) break
            val name = line.substringBefore(':').trim()
            if (name.equals("range", true)) rawRange = line.substringAfter(':').trim()
        }
        return HttpRequest(parts[0].uppercase(), parts[1], parseRange(rawRange), rawRange)
    }

    private fun InputStream.readHttpLine(): String? {
        // HTTP header bytes are ISO-8859-1.
        val bytes = java.io.ByteArrayOutputStream()
        while (true) {
            val b = read()
            if (b == -1) return if (bytes.size() == 0) null else bytes.toString("ISO-8859-1")
            if (b == '\n'.code) break
            // Drop the CR so CRLF and a bare LF both end a line.
            if (b != '\r'.code) bytes.write(b)
            // Refuse absurdly long lines instead of buffering them.
            if (bytes.size() > 8192) return null
        }
        return bytes.toString("ISO-8859-1")
    }

    // Only "bytes=N-" and "bytes=N-M" are understood. Anything else is null, so files and Raw
    // streams answer with the whole body. Proxied requests still pass the raw header upstream.
    private fun parseRange(header: String?): Range? {
        // Any unit other than bytes.
        val spec = header?.removePrefix("bytes=")?.takeIf { it != header } ?: return null
        // Multi-range requests. Sending the whole body is a valid answer to them.
        if (spec.contains(',')) return null
        // A suffix range (bytes=-500) has no start.
        val start = spec.substringBefore('-').trim().toLongOrNull() ?: return null
        // bytes=100- is open ended, so the end stays null.
        val end = spec.substringAfter('-').trim().toLongOrNull()
        return Range(start, end)
    }

    private fun respond(request: HttpRequest, output: OutputStream) {
        // CORS preflight, the CORS headers are added by writeHead().
        if (request.method == "OPTIONS") return output.writeHead(204, "No Content")
        if (request.method != "GET" && request.method != "HEAD")
            return output.writeHead(405, "Method Not Allowed")
        val segments = request.path.substringBefore('?').trim('/').split('/')
        val token = segments.getOrNull(1)
        // Paths look like /track/<token> or /art/<token>, anything else is unknown.
        if (segments.size != 2 || token == null || !items.containsKey(token))
            return output.writeHead(404, "Not Found")
        when (segments[0]) {
            TRACK -> {
                // Blocks this connection's thread until the track is resolved. If a prefetch is
                // already running, this just joins it.
                val loaded = runCatching { runBlocking { load(token).await() } }.getOrElse {
                    Log.w(TAG, "Could not resolve track ${token.take(8)}", it)
                    // The receiver only gets a plain 502, the cause is in the log.
                    return output.writeHead(502, "Bad Gateway")
                }
                serveSource(token, loaded.source, request, output)
            }

            ART -> serveArt(items[token]!!, request, output)
            else -> output.writeHead(404, "Not Found")
        }
    }

    private fun serveSource(
        token: String, source: Streamable.Source, request: HttpRequest, output: OutputStream,
    ) {
        when (source) {
            is Streamable.Source.Raw -> serveRaw(token, source, request, output)
            is Streamable.Source.Http -> {
                val uri = source.request.url.toUri()
                // file: and content: URIs only exist on the phone, so they are streamed from here.
                val isLocal = uri.scheme == "file" || uri.scheme == "content"
                if (isLocal) serveLocal(uri, request, output)
                else proxy(source.request, request, output, DEFAULT_TYPE)
            }
        }
    }

    private fun serveRaw(
        token: String, source: Streamable.Source.Raw, request: HttpRequest, output: OutputStream,
    ) {
        val provider = source.streamProvider!!
        // Opens the stream at [position]. The second value is how many bytes remain, negative
        // when the provider does not know.
        fun openAt(position: Long) = runCatching {
            runBlocking { provider.provide(position, -1) }
        }.getOrElse { throw SourceException("Could not open the stream at byte $position", it) }

        val start = request.range?.start ?: 0
        val (stream, remaining) = openAt(start)
        // Buffered so sniffType() can peek at the first bytes and rewind.
        BufferedInputStream(stream, BUFFER_SIZE).use { input ->
            // The provider starts at [start], so the full size is that offset plus what remains.
            val total = if (remaining >= 0) start + remaining else -1
            // Unknown size and a seek: there is no valid Content-Range to answer with.
            if (total < 0 && start > 0) return output.writeHead(416, "Range Not Satisfiable")
            // The queue told the receiver audio/mpeg, so the real type has to come from the data.
            // Range requests start mid-file and reuse what the first request found.
            val type = if (start == 0L) sniffType(input)?.also { types[token] = it }
            else types[token]
            // The provider already starts at the requested byte, so nothing is skipped here.
            serveStream(
                input, total, start, request, output, type ?: DEFAULT_TYPE, alreadySkipped = true
            ) { position -> openAt(position).first }
        }
    }

    private fun serveLocal(
        uri: Uri, request: HttpRequest, output: OutputStream, fallbackType: String = DEFAULT_TYPE,
    ) {
        val resolver = app.context.contentResolver
        val descriptor = resolver.openAssetFileDescriptor(uri, "r")
            ?: return output.writeHead(404, "Not Found")
        descriptor.use { afd ->
            // Some providers report no length, then the file size is the best answer.
            val total = afd.length.takeIf { it >= 0 } ?: afd.parcelFileDescriptor.statSize
            BufferedInputStream(afd.createInputStream(), BUFFER_SIZE).use { input ->
                // Embedded album art from Android often has no type and no file extension. So try
                // the resolver, then the first bytes, then the extension. The resolver's answer
                // only counts when it is audio, video or image.
                val type = resolver.getType(uri)
                    ?.takeIf { type -> MEDIA_TYPES.any { type.startsWith(it) } }
                    ?: sniffType(input)
                    ?: guessType(uri.toString()).takeIf { it != DEFAULT_TYPE }
                    ?: fallbackType
                serveStream(input, total, request.range?.start ?: 0, request, output, type)
            }
        }
    }

    private fun serveStream(
        input: InputStream,
        total: Long,
        start: Long,
        request: HttpRequest,
        output: OutputStream,
        type: String,
        alreadySkipped: Boolean = false,
        reopen: ((position: Long) -> InputStream)? = null,
    ) {
        val headers = mutableMapOf("Content-Type" to type, "Accept-Ranges" to "bytes")
        // Start is at or past the end. "bytes */total" tells the client the real size.
        if (total >= 0 && start >= total && total > 0) {
            headers["Content-Range"] = "bytes */$total"
            return output.writeHead(416, "Range Not Satisfiable", headers)
        }
        val length = if (request.range != null && total >= 0) {
            // Bytes are inclusive, and an open or oversized end is clamped to the last byte.
            val end = (request.range.end ?: (total - 1)).coerceAtMost(total - 1)
            headers["Content-Range"] = "bytes $start-$end/$total"
            headers["Content-Length"] = "${end - start + 1}"
            output.writeHead(206, "Partial Content", headers)
            end - start + 1
        } else {
            // No Range, or a size we do not know: plain 200. Without a size there is no
            // Content-Length and the body ends when the connection closes.
            if (total >= 0) headers["Content-Length"] = "$total"
            output.writeHead(200, "OK", headers)
            total
        }
        // HEAD gets the headers only.
        if (request.method == "HEAD") return
        // Other sources start at byte 0 and have to be advanced to the requested offset.
        if (!alreadySkipped) input.skipFully(start)
        copyResuming(input, start, length, output, reopen)
    }

    private fun proxy(
        networkRequest: NetworkRequest,
        request: HttpRequest,
        output: OutputStream,
        fallbackType: String,
    ) {
        // The receiver's Range goes upstream as it came, so a seek does not download from byte 0.
        val response = runCatching {
            client.newCall(upstreamRequest(networkRequest, request.rawRange)).execute()
        }.getOrElse { throw SourceException("Could not reach the source", it) }
        response.use {
            if (!response.isSuccessful) {
                Log.w(TAG, "The source answered HTTP ${response.code}")
                // An upstream error, including 416, reaches the receiver as a 502.
                return output.writeHead(502, "Bad Gateway")
            }
            val headers = mutableMapOf<String, String>()
            // The source's type is only trusted when it is audio, video or image. Otherwise guess
            // from the URL, then fall back.
            headers["Content-Type"] = response.header("Content-Type")
                ?.takeIf { type -> MEDIA_TYPES.any { type.startsWith(it) } }
                ?: guessType(networkRequest.url).takeIf { it != DEFAULT_TYPE }
                ?: fallbackType
            // Length and range headers pass through unchanged, the receiver needs them to seek.
            response.header("Content-Length")?.let { headers["Content-Length"] = it }
            response.header("Content-Range")?.let { headers["Content-Range"] = it }
            response.header("Accept-Ranges")?.let { headers["Accept-Ranges"] = it }
            val code = response.code
            output.writeHead(code, if (code == 206) "Partial Content" else "OK", headers)
            if (request.method == "HEAD") return
            // Where this response sits in the file, so a broken stream can be resumed at the
            // right byte.
            val range = response.header("Content-Range")?.let { parseContentRange(it) }
            val start = range?.start ?: 0
            // Negative when the source did not declare a length.
            val length = response.body.contentLength()
            // Last byte of this response, null when it cannot be worked out.
            val end = range?.end ?: if (length >= 0) start + length - 1 else null
            copyResuming(response.body.byteStream(), start, length, output) { position ->
                reopenSource(networkRequest, position, end)
            }
        }
    }

    // Asks the source for the rest of a response that broke off at [position].
    private fun reopenSource(
        networkRequest: NetworkRequest, position: Long, end: Long?,
    ): InputStream {
        // An unknown end leaves the range open, so the source sends everything that remains.
        val response = runCatching {
            client.newCall(upstreamRequest(networkRequest, "bytes=$position-${end ?: ""}"))
                .execute()
        }.getOrElse { throw SourceException("Could not reach the source again", it) }
        // Only a 206 that starts at [position] can be spliced in. A plain 200 means the source
        // ignored Range and would replay the file from byte 0 into the middle of the response.
        val resumed = response.code == 206 &&
                response.header("Content-Range")?.startsWith("bytes $position-") == true
        if (!resumed) {
            response.close()
            throw SourceException("The source cannot resume at byte $position (HTTP ${response.code})")
        }
        return response.body.byteStream()
    }

    private fun upstreamRequest(networkRequest: NetworkRequest, range: String?): Request {
        val method = networkRequest.method.name
        val body = networkRequest.body?.toRequestBody()
        // OkHttp rejects a body on GET and HEAD.
        val builder = Request.Builder().url(networkRequest.url)
            .method(method, body.takeIf { method != "GET" && method != "HEAD" })
        // These are the headers the extension needs, the reason the receiver cannot fetch directly.
        networkRequest.headers.forEach { (key, value) -> builder.header(key, value) }
        // With no Range header OkHttp asks for gzip by itself, and resume offsets would then
        // count decompressed bytes. Identity keeps them equal to the bytes we forward.
        if (networkRequest.headers.keys.none { it.equals("Accept-Encoding", true) })
            builder.header("Accept-Encoding", "identity")
        range?.let { builder.header("Range", it) }
        return builder.build()
    }

    private fun parseContentRange(header: String): Range? {
        // Format is "bytes start-end/total". The total is not needed here.
        val spec = header.removePrefix("bytes ").substringBefore('/')
        val start = spec.substringBefore('-').trim().toLongOrNull() ?: return null
        return Range(start, spec.substringAfter('-', "").trim().toLongOrNull())
    }

    private fun serveArt(item: MediaItem, request: HttpRequest, output: OutputStream) {
        // Images are fetched whole, so any Range header is dropped.
        when (val cover = runCatching { item.track.cover }.getOrNull()) {
            is ImageHolder.NetworkRequestImageHolder ->
                proxy(cover.request, request.copyWithoutRange(), output, "image/jpeg")

            is ImageHolder.ResourceUriImageHolder ->
                serveLocal(cover.uri.toUri(), request.copyWithoutRange(), output, "image/jpeg")
            else -> output.writeHead(404, "Not Found")
        }
    }

    private fun HttpRequest.copyWithoutRange() = HttpRequest(method, path, null, null)

    private fun OutputStream.writeHead(
        code: Int, reason: String, headers: Map<String, String> = emptyMap(),
    ) {
        val head = StringBuilder("HTTP/1.1 $code $reason\r\n")
        // CORS on every reply, a web based receiver reads media from another origin.
        (headers + CORS_HEADERS).forEach { (key, value) -> head.append("$key: $value\r\n") }
        // Error replies have no body. Saying so stops the client waiting for data.
        if (!headers.containsKey("Content-Length") && code !in 200..299)
            head.append("Content-Length: 0\r\n")
        // One request per connection, the socket is closed after the reply.
        head.append("Connection: close\r\n\r\n")
        write(head.toString().toByteArray(Charsets.ISO_8859_1))
        // Send the head right away, the body may take a while to start.
        flush()
        // Length for the log: -1 for a 2xx body of unknown size, 0 for an error without a body.
        val declared = headers["Content-Length"]?.toLongOrNull() ?: if (code in 200..299) -1 else 0
        (this as? ResponseStream)?.onHeadSent(code, declared)
    }

    private fun InputStream.skipFully(count: Long) {
        var left = count
        while (left > 0) {
            // skip() may skip nothing. Reading one byte tells a stalled stream from its end.
            val skipped = skip(left)
            if (skipped > 0) left -= skipped
            else if (read() == -1) return
            else left--
        }
    }

    /**
     * Copies [length] bytes from [first], or until it ends when the length is unknown. [first]
     * starts at byte [start] of the file. If the source fails or ends early, [reopen] fetches the
     * rest from where it stopped, so the receiver never sees a truncated body. Chromecast reads
     * in bursts and may leave a connection idle for tens of seconds, so sources do drop theirs.
     * Errors writing to [output] belong to the receiver and are not retried.
     */
    private fun copyResuming(
        first: InputStream,
        start: Long,
        length: Long,
        output: OutputStream,
        reopen: ((position: Long) -> InputStream)?,
    ) {
        val buffer = ByteArray(BUFFER_SIZE)
        var input = first
        var sent = 0L
        var resumes = 0
        // Gives up after MAX_RESUMES reopens, or once the thread is interrupted by stop().
        fun canResume() =
            reopen != null && resumes < MAX_RESUMES && !Thread.currentThread().isInterrupted
        try {
            while (length < 0 || sent < length) {
                // Never read past the declared length, so the body matches Content-Length.
                val wanted = if (length < 0) buffer.size
                else minOf(buffer.size.toLong(), length - sent).toInt()
                val read = try {
                    input.read(buffer, 0, wanted)
                } catch (e: IOException) {
                    // Read errors come from the source. Writes to the receiver are further down.
                    val at = start + sent
                    if (!canResume()) throw SourceException("The stream failed at byte $at", e)
                    Log.w(TAG, "The stream failed at byte $at, reopening it: $e")
                    READ_FAILED
                }
                if (read == -1) {
                    // With no declared length, end of stream is the real end.
                    if (length < 0) break
                    // Ended before the declared length, so the response would be truncated.
                    val at = "byte ${start + sent} instead of ${start + length}"
                    if (!canResume()) throw SourceException("The stream ended at $at")
                    Log.w(TAG, "The stream ended at $at, reopening it")
                }
                // Both a failed read and an early end land here.
                if (read < 0) {
                    resumes++
                    // first is closed by whoever opened it, only streams opened here are closed here.
                    if (input !== first) runCatching { input.close() }
                    input = first // Not closed twice if reopening fails.
                    // Continue at the first byte the receiver has not got yet.
                    input = reopen!!(start + sent)
                    continue
                }
                // A failure here is the receiver's. It propagates and is not retried.
                output.write(buffer, 0, read)
                sent += read
            }
        } finally {
            // Also runs when the receiver hung up, so a reopened source is not leaked.
            if (input !== first) runCatching { input.close() }
        }
        output.flush()
    }

    // Guesses the type from the first bytes: common audio containers and cover image formats.
    private fun sniffType(input: BufferedInputStream): String? {
        // 12 bytes reach the longest signature checked below (WEBP and WAVE at offset 8).
        val head = ByteArray(12)
        // Peek only, reset() below puts the stream back at its first byte.
        input.mark(head.size)
        var count = 0
        try {
            // read() can return fewer bytes than asked for, so keep going until full.
            while (count < head.size) {
                val read = input.read(head, count, head.size - count)
                if (read < 0) break
                count += read
            }
        } catch (e: IOException) {
            // Reported as a source failure so the log does not blame the receiver.
            throw SourceException("Could not read the start of the stream", e)
        }
        input.reset()
        fun byteAt(i: Int) = head[i].toInt() and 0xFF
        fun textAt(offset: Int, text: String) = count >= offset + text.length &&
                text.indices.all { head[offset + it] == text[it].code.toByte() }
        return when {
            count >= 3 && byteAt(0) == 0xFF && byteAt(1) == 0xD8 && byteAt(2) == 0xFF -> "image/jpeg"
            count >= 4 && byteAt(0) == 0x89 && textAt(1, "PNG") -> "image/png"
            textAt(0, "RIFF") && textAt(8, "WEBP") -> "image/webp"
            textAt(0, "GIF8") -> "image/gif"
            textAt(0, "fLaC") -> "audio/flac"
            textAt(0, "OggS") -> "audio/ogg"
            // MP4 and M4A start with a box size, then the "ftyp" tag.
            textAt(4, "ftyp") -> "audio/mp4"
            textAt(0, "RIFF") && textAt(8, "WAVE") -> "audio/wav"
            // MP3 with an ID3 tag in front of the first frame.
            textAt(0, "ID3") -> "audio/mpeg"
            // EBML header, used by WebM and Matroska.
            count >= 4 && byteAt(0) == 0x1A && byteAt(1) == 0x45 &&
                    byteAt(2) == 0xDF && byteAt(3) == 0xA3 -> "audio/webm"
            // ADTS (AAC) and MPEG audio both start with a frame sync. AAC has layer bits 00, which
            // MPEG audio never uses, so it has to be tested first.
            count >= 2 && byteAt(0) == 0xFF && (byteAt(1) and 0xF6) == 0xF0 -> "audio/aac"
            count >= 2 && byteAt(0) == 0xFF && (byteAt(1) and 0xE0) == 0xE0 -> "audio/mpeg"
            else -> null
        }
    }

    private fun guessType(url: String): String {
        val extension = MimeTypeMap.getFileExtensionFromUrl(url)
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension) ?: DEFAULT_TYPE
    }

    companion object {
        private const val TAG = "CastStreamServer"
        private const val TRACK = "track"
        private const val ART = "art"
        private const val BUFFER_SIZE = 64 * 1024
        // Reopens allowed per response before the failure is passed on.
        private const val MAX_RESUMES = 5

        // Internal marker for a failed read, kept apart from -1 which is end of stream.
        private const val READ_FAILED = -2

        // TODO: the receiver is told every track is audio/mpeg when it is queued, because the
        // real type is only known after the track has been resolved.
        const val DEFAULT_TYPE = "audio/mpeg"
        private val MEDIA_TYPES = listOf("audio/", "video/", "image/")

        private val CORS_HEADERS = mapOf(
            "Access-Control-Allow-Origin" to "*",
            "Access-Control-Allow-Methods" to "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers" to "Range, Content-Type",
            "Access-Control-Expose-Headers" to "Content-Length, Content-Range, Accept-Ranges",
        )
    }
}
