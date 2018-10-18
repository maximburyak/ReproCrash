using System;

namespace Voron.Platform.Posix
{
    [Flags]
    public enum ProtFlag : int
    {
                PROT_READ = 0x1,     /* Page can be read.  */     
    }
}