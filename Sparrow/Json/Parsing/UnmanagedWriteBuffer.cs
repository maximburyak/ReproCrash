using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;

namespace Sparrow.Json.Parsing
{
    public unsafe class UnmanagedWriteBuffer
    {        
        private class Segment
        {
            /// <summary>
            /// Memory in this Segment
            /// </summary>
            public AllocatedMemoryData Allocation;

            /// <summary>
            /// Always set to Allocation.Adddress
            /// </summary>
            public byte* Address;

            /// <summary>
            /// Used bytes in the current Segment
            /// </summary>
            public int Used;

            /// <summary>
            /// Total size accumulated by all the previous Segments
            /// </summary>
            public int AccumulatedSizeInBytes;
          
        }

        private Segment _head;

        public int SizeInBytes
        {
            get
            {
                ThrowOnDisposed();
                return _head.AccumulatedSizeInBytes;
            }
        }

        // Since we never know which instance actually ran the Dispose, it is 
        // possible that this particular copy may have _head != null.
        public bool IsDisposed => _head == null || _head.Address == null;

        public UnmanagedWriteBuffer(AllocatedMemoryData allocatedMemoryData)
        {            
            _head = new Segment
            {                
                Allocation = allocatedMemoryData,
                Address = allocatedMemoryData.Address,
                Used = 0,
                AccumulatedSizeInBytes = 0
            };

        }       

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowOnDisposed()
        {

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(byte* buffer, int count)
        {            
            ThrowOnDisposed();

            if (count == 0)
                return;

            var head = _head;
            if (head.Allocation.SizeInBytes - head.Used > count)
            {                
                Unsafe.CopyBlock(head.Address + head.Used, buffer, (uint)count);
                head.AccumulatedSizeInBytes += count;
                head.Used += count;
                Syscall.mprotect((IntPtr)head.Address, (ulong)count, ProtFlag.PROT_READ);
            }
            else
            {
                Console.WriteLine("ERRR");
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte data)
        {
            ThrowOnDisposed();
            return;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            ThrowOnDisposed();
            _head.Used = 0;
            _head.AccumulatedSizeInBytes = 0;            
        }

        public void Dispose()
        {
            if (IsDisposed)
                return;

            // The actual lifetime of the _head Segment is unbounded: it will
            // be released by the GC when we no longer have any references to
            // it (i.e. no more copies of this struct)
            //
            // We can, however, force the deallocation of all the previous
            // Segments by ensuring we don't keep any references after the
            // Dispose is run.            
            _head.Address = null;
            _head = null; // Further references are NREs.            
            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void EnsureSingleChunk(JsonParserState state)
        {            
            EnsureSingleChunk(out state.StringBuffer, out state.StringSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void EnsureSingleChunk(out byte* ptr, out int size)
        {
            ThrowOnDisposed();               
            ptr = _head.Address;
            size = SizeInBytes;           
        }
    }
}