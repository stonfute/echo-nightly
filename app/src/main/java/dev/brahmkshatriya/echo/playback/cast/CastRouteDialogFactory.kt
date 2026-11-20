package dev.brahmkshatriya.echo.playback.cast

import android.content.Context
import android.os.Bundle
import androidx.mediarouter.app.MediaRouteChooserDialog
import androidx.mediarouter.app.MediaRouteChooserDialogFragment
import androidx.mediarouter.app.MediaRouteDialogFactory
import androidx.mediarouter.app.MediaRouteDynamicChooserDialog
import androidx.mediarouter.media.MediaRouter
import com.google.android.gms.common.GoogleApiAvailability

/**
 * Cast device picker that only lists the devices Google Play services publishes.
 *
 * MediaRouter shows the routes of every installed provider. Play services clones made for
 * patched apps (ReVanced GmsCore, microG) publish the same Cast devices again, so each device
 * showed up twice. Picking one of those copies can't work anyway: our CastContext only talks to
 * Google Play services.
 */
class CastRouteDialogFactory : MediaRouteDialogFactory() {

    override fun onCreateChooserDialogFragment(): MediaRouteChooserDialogFragment =
        ChooserFragment()

    // Public with an empty constructor, Android recreates it after a configuration change.
    class ChooserFragment : MediaRouteChooserDialogFragment() {
        override fun onCreateChooserDialog(
            context: Context, savedInstanceState: Bundle?,
        ): MediaRouteChooserDialog = object : MediaRouteChooserDialog(context) {
            override fun onFilterRoute(route: MediaRouter.RouteInfo) =
                super.onFilterRoute(route) && route.isFromPlayServices()
        }

        // MediaRouter picks this one instead when dynamic groups are enabled.
        override fun onCreateDynamicChooserDialog(context: Context): MediaRouteDynamicChooserDialog =
            object : MediaRouteDynamicChooserDialog(context) {
                override fun onFilterRoute(route: MediaRouter.RouteInfo) =
                    super.onFilterRoute(route) && route.isFromPlayServices()
            }
    }

    companion object {
        private fun MediaRouter.RouteInfo.isFromPlayServices() =
            provider.packageName == GoogleApiAvailability.GOOGLE_PLAY_SERVICES_PACKAGE
    }
}
