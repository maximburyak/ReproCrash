using System;
using System.Runtime.InteropServices;
using System.Security;

namespace Sparrow.Platform.Posix
{
    public static unsafe class Syscall
    {
        internal const string LIBC_6 = "libc";

        [DllImport(LIBC_6, EntryPoint = "memcpy", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode, SetLastError = false)]
        [SecurityCritical]
        public static extern IntPtr Copy(byte* dest, byte* src, long count);

        [DllImport(LIBC_6, EntryPoint = "memcmp", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        public static extern int Compare(byte* b1, byte* b2, long count);
    }
}
