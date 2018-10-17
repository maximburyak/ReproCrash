using System.Runtime.InteropServices;
using static Sparrow.Platform.PlatformDetails;

namespace Voron.Platform.Posix
{
    public class PerPlatformValues
    {
        public class SyscallNumbers
        {
            public static long SYS_gettid =
            (RuntimeInformation.OSArchitecture == Architecture.Arm ||
             RuntimeInformation.OSArchitecture == Architecture.Arm64)
                ? 224
                : 186;
        }
    }
}