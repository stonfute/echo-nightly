package dev.brahmkshatriya.echo.playback.cast

import android.content.Context
import androidx.annotation.Keep
import com.google.android.gms.cast.CastMediaControlIntent
import com.google.android.gms.cast.LaunchOptions
import com.google.android.gms.cast.framework.CastOptions
import com.google.android.gms.cast.framework.OptionsProvider
import com.google.android.gms.cast.framework.SessionProvider
import com.google.android.gms.cast.framework.media.CastMediaOptions

// Only referenced by name from the manifest, so R8 has to be told to keep it.
@Keep
class CastOptionsProvider : OptionsProvider {
    override fun getCastOptions(context: Context): CastOptions {
        // The media session, notification and lock screen controls already come from
        // PlayerService, so the Cast framework must not create its own.
        val mediaOptions = CastMediaOptions.Builder()
            .setNotificationOptions(null)
            .setMediaSessionEnabled(false)
            .build()
        // Always relaunch the receiver so we never join a queue left by another sender.
        val launchOptions = LaunchOptions.Builder()
            .setRelaunchIfRunning(true)
            .build()
        // Google's Default Media Receiver, so there is no receiver app of our own to host.
        // Every cast starts fresh: old sessions aren't resumed, and ending the session closes
        // the receiver.
        return CastOptions.Builder()
            .setReceiverApplicationId(CastMediaControlIntent.DEFAULT_MEDIA_RECEIVER_APPLICATION_ID)
            .setCastMediaOptions(mediaOptions)
            .setLaunchOptions(launchOptions)
            .setResumeSavedSession(false)
            .setEnableReconnectionService(false)
            .setStopReceiverApplicationWhenEndingSession(true)
            .build()
    }

    override fun getAdditionalSessionProviders(context: Context): List<SessionProvider>? = null
}
