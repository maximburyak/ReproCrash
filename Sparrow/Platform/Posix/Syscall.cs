using System;
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using System.Threading;
using Voron.Platform.Posix;

namespace Sparrow.Platform.Posix
{
    public static unsafe class Syscall
    {
        internal const string LIBC_6 = "libc";
 
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int mprotect(IntPtr start, ulong size, ProtFlag protFlag);   
      
    } 
}
