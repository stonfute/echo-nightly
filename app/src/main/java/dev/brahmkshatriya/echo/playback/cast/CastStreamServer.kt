package dev.brahmkshatriya.echo.playback.cast

import android.net.Uri
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
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * A small HTTP server that lets a Cast receiver play Echo tracks.
 *
 * A Cast device fetches media by itself, so it cannot use streams that need extension headers,
 * [Streamable.Source.Raw] sources or downloaded files. Each queued track is registered under a
 * random token, and the receiver is given a URL on this server instead. The track is resolved
 * through the same [StreamableLoader] as local playback the first time the receiver asks for it,
 * then proxied with range support so seeking works.
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

    private val items = ConcurrentHashMap<String, MediaItem>()
    private val loads = ConcurrentHashMap<String, Deferred<Loaded>>()

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

    fun clear() {
        items.clear()
        loads.values.forEach { it.cancel() }
        loads.clear()
    }

    fun trackUrl(token: String) = "${requireNotNull(baseUrl)}/$TRACK/$token"
    fun artUrl(token: String) = "${requireNotNull(baseUrl)}/$ART/$token"

    fun tokenOf(url: String?): String? {
        val token = url?.substringAfterLast('/') ?: return null
        return token.takeIf { items.containsKey(it) }
    }

    fun hasArt(item: MediaItem) = when (runCatching { item.track.cover }.getOrNull()) {
        is ImageHolder.NetworkRequestImageHolder, is ImageHolder.ResourceUriImageHolder -> true
        else -> false
    }

    /**
     * Starts listening on [host], the address of this device on the network the Cast receiver is
     * on. Binding to that address only keeps the server off other interfaces such as mobile data.
     */
    fun start(host: InetAddress) {
        stop()
        val socket = ServerSocket(0, 50, host)
        val pool = Executors.newCachedThreadPool {
            Thread(it, "CastStreamServer").apply { isDaemon = true }
        }
        serverSocket = socket
        executor = pool
        val hostName = host.hostAddress!!.substringBefore('%')
        val urlHost = if (hostName.contains(':')) "[$hostName]" else hostName
        baseUrl = "http://$urlHost:${socket.localPort}"
        pool.execute {
            while (!socket.isClosed) {
                val connection = runCatching { socket.accept() }.getOrNull() ?: break
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
            runCatching {
                val (loaded, server) = loader.load(item)
                state.servers[loaded.mediaId] = server
                state.serverChanged.emit(Unit)
                val (source, new) = selectSource(loaded, server.getOrThrow())
                items.computeIfPresent(token) { _, _ -> new }
                listener.onLoaded(token, item, new)
                Loaded(new, source)
            }.getOrElse {
                if (it !is CancellationException) listener.onFailed(token, item, it)
                throw it
            }
        }
    }

    // Mirrors the source selection of StreamableMediaSource, skipping sources a Cast device
    // cannot play through this server.
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

    private fun handle(socket: Socket) = socket.use {
        runCatching {
            socket.soTimeout = 30_000
            val input = BufferedInputStream(socket.getInputStream())
            val output = BufferedOutputStream(socket.getOutputStream())
            val request = readRequest(input) ?: return@use
            respond(request, output)
            output.flush()
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
        val bytes = java.io.ByteArrayOutputStream()
        while (true) {
            val b = read()
            if (b == -1) return if (bytes.size() == 0) null else bytes.toString("ISO-8859-1")
            if (b == '\n'.code) break
            if (b != '\r'.code) bytes.write(b)
            if (bytes.size() > 8192) return null
        }
        return bytes.toString("ISO-8859-1")
    }

    private fun parseRange(header: String?): Range? {
        val spec = header?.removePrefix("bytes=")?.takeIf { it != header } ?: return null
        if (spec.contains(',')) return null
        val start = spec.substringBefore('-').trim().toLongOrNull() ?: return null
        val end = spec.substringAfter('-').trim().toLongOrNull()
        return Range(start, end)
    }

    private fun respond(request: HttpRequest, output: OutputStream) {
        if (request.method == "OPTIONS") return output.writeHead(204, "No Content")
        if (request.method != "GET" && request.method != "HEAD")
            return output.writeHead(405, "Method Not Allowed")
        val segments = request.path.substringBefore('?').trim('/').split('/')
        val token = segments.getOrNull(1)
        if (segments.size != 2 || token == null || !items.containsKey(token))
            return output.writeHead(404, "Not Found")
        when (segments[0]) {
            TRACK -> {
                val loaded = runCatching { runBlocking { load(token).await() } }.getOrElse {
                    return output.writeHead(502, "Bad Gateway")
                }
                serveSource(loaded.source, request, output)
            }

            ART -> serveArt(items[token]!!, request, output)
            else -> output.writeHead(404, "Not Found")
        }
    }

    private fun serveSource(source: Streamable.Source, request: HttpRequest, output: OutputStream) {
        when (source) {
            is Streamable.Source.Raw -> serveRaw(source, request, output)
            is Streamable.Source.Http -> {
                val uri = source.request.url.toUri()
                val isLocal = uri.scheme == "file" || uri.scheme == "content"
                if (isLocal) serveLocal(uri, request, output)
                else proxy(source.request, request, output, DEFAULT_TYPE)
            }
        }
    }

    private fun serveRaw(
        source: Streamable.Source.Raw, request: HttpRequest, output: OutputStream,
    ) {
        val start = request.range?.start ?: 0
        val (stream, remaining) = runBlocking {
            source.streamProvider!!.provide(start, -1)
        }
        stream.use {
            val total = if (remaining >= 0) start + remaining else -1
            if (total < 0 && start > 0) return output.writeHead(416, "Range Not Satisfiable")
            serveStream(it, total, start, request, output, DEFAULT_TYPE, alreadySkipped = true)
        }
    }

    private fun serveLocal(uri: Uri, request: HttpRequest, output: OutputStream) {
        val resolver = app.context.contentResolver
        val descriptor = resolver.openAssetFileDescriptor(uri, "r")
            ?: return output.writeHead(404, "Not Found")
        descriptor.use { afd ->
            val total = afd.length.takeIf { it >= 0 } ?: afd.parcelFileDescriptor.statSize
            val type = resolver.getType(uri) ?: guessType(uri.toString())
            afd.createInputStream().use {
                serveStream(it, total, request.range?.start ?: 0, request, output, type)
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
    ) {
        val headers = mutableMapOf("Content-Type" to type, "Accept-Ranges" to "bytes")
        if (total >= 0 && start >= total && total > 0) {
            headers["Content-Range"] = "bytes */$total"
            return output.writeHead(416, "Range Not Satisfiable", headers)
        }
        val length = if (request.range != null && total >= 0) {
            val end = (request.range.end ?: (total - 1)).coerceAtMost(total - 1)
            headers["Content-Range"] = "bytes $start-$end/$total"
            headers["Content-Length"] = "${end - start + 1}"
            output.writeHead(206, "Partial Content", headers)
            end - start + 1
        } else {
            if (total >= 0) headers["Content-Length"] = "$total"
            output.writeHead(200, "OK", headers)
            total
        }
        if (request.method == "HEAD") return
        if (!alreadySkipped) input.skipFully(start)
        input.copyTo(output, length)
    }

    private fun proxy(
        networkRequest: NetworkRequest,
        request: HttpRequest,
        output: OutputStream,
        fallbackType: String,
    ) {
        val method = networkRequest.method.name
        val body = networkRequest.body?.toRequestBody()
        val builder = Request.Builder().url(networkRequest.url)
            .method(method, body.takeIf { method != "GET" && method != "HEAD" })
        networkRequest.headers.forEach { (key, value) -> builder.header(key, value) }
        request.rawRange?.let { builder.header("Range", it) }
        client.newCall(builder.build()).execute().use { response ->
            if (!response.isSuccessful) return output.writeHead(502, "Bad Gateway")
            val headers = mutableMapOf<String, String>()
            headers["Content-Type"] = response.header("Content-Type")
                ?.takeIf { type -> MEDIA_TYPES.any { type.startsWith(it) } }
                ?: guessType(networkRequest.url).takeIf { it != DEFAULT_TYPE }
                ?: fallbackType
            response.header("Content-Length")?.let { headers["Content-Length"] = it }
            response.header("Content-Range")?.let { headers["Content-Range"] = it }
            response.header("Accept-Ranges")?.let { headers["Accept-Ranges"] = it }
            val code = response.code
            output.writeHead(code, if (code == 206) "Partial Content" else "OK", headers)
            if (request.method == "HEAD") return
            response.body.byteStream().use { it.copyTo(output, -1) }
        }
    }

    private fun serveArt(item: MediaItem, request: HttpRequest, output: OutputStream) {
        when (val cover = runCatching { item.track.cover }.getOrNull()) {
            is ImageHolder.NetworkRequestImageHolder ->
                proxy(cover.request, request.copyWithoutRange(), output, "image/jpeg")

            is ImageHolder.ResourceUriImageHolder -> serveLocal(cover.uri.toUri(), request, output)
            else -> output.writeHead(404, "Not Found")
        }
    }

    private fun HttpRequest.copyWithoutRange() = HttpRequest(method, path, null, null)

    private fun OutputStream.writeHead(
        code: Int, reason: String, headers: Map<String, String> = emptyMap(),
    ) {
        val head = StringBuilder("HTTP/1.1 $code $reason\r\n")
        (headers + CORS_HEADERS).forEach { (key, value) -> head.append("$key: $value\r\n") }
        if (!headers.containsKey("Content-Length") && code !in 200..299)
            head.append("Content-Length: 0\r\n")
        head.append("Connection: close\r\n\r\n")
        write(head.toString().toByteArray(Charsets.ISO_8859_1))
        flush()
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

    private fun InputStream.copyTo(output: OutputStream, length: Long) {
        val buffer = ByteArray(64 * 1024)
        var left = if (length < 0) Long.MAX_VALUE else length
        while (left > 0) {
            val read = read(buffer, 0, minOf(buffer.size.toLong(), left).toInt())
            if (read == -1) break
            output.write(buffer, 0, read)
            left -= read
        }
        output.flush()
    }

    private fun guessType(url: String): String {
        val extension = MimeTypeMap.getFileExtensionFromUrl(url)
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension) ?: DEFAULT_TYPE
    }

    companion object {
        private const val TRACK = "track"
        private const val ART = "art"
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
