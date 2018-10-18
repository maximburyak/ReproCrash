using System;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace SmallerRepro
{

     [Flags]
    public enum ProtFlag : int
    {
                PROT_READ = 0x1,     /* Page can be read.  */     
    }

    public static unsafe class Syscall
    {
        internal const string LIBC_6 = "libc";
 
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int mprotect(IntPtr start, ulong size, ProtFlag protFlag);         
    } 

    class Program
    {
        static unsafe void Main(string[] args)
        {            
            var charArray = new char[8*1024*1024+5];
            for (var i=0; i< charArray.Length; i++)
            {
                charArray[i] = (char)(i%255);
            }                    

            for (int ss = 0; ss < 100; ss++)
            {                
                Console.WriteLine("************ " + ss + " ***********");
                Parallel.For(0, 20, a =>
                {
                    var random = new Random();
                   
                    string readAllText = new string(charArray);
                    
                    byte[] filebytes1 = Encoding.UTF8.GetBytes(readAllText);

                    for ( var i=0; i< 100; i++)
                    {
                        var size = random.Next(1024*1024 -1, 10*1024*1024 -1);
                        var address = (byte*) Marshal.AllocHGlobal(size);
                        Syscall.mprotect((IntPtr)address, (ulong)size, ProtFlag.PROT_READ);                         
                        var subString = readAllText.Substring(random.Next(1,readAllText.Length));                         
                    }                    
                    GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);                
                });
                
            }        
        }
    }
}
