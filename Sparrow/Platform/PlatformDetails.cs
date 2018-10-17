using System;
using System.Runtime.InteropServices;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;

namespace Sparrow.Platform
{
    public static class PlatformDetails
    {
        public static readonly bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                                     RuntimeInformation.IsOSPlatform(OSPlatform.OSX);     
    }
}
