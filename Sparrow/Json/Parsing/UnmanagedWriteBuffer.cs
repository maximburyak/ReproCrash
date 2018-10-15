using System;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Platform.Posix;

namespace Sparrow.Json.Parsing
{
    public unsafe struct UnmanagedWriteBuffer
    {
        private readonly JsonOperationContext _context;

        private class Segment
        {
            /// <summary>
            /// This points to the previous Segment in the stream. May be null
            /// due either to a Clean operation, or because none have been
            /// allocated
            /// </summary>
            public Segment Previous;

            /// <summary>
            /// Every Segment in this linked list is freed when the 
            /// UnmanagedWriteBuffer is disposed. Kept for resilience against
            /// Clean operations
            /// </summary>
            public Segment DeallocationPendingPrevious;

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

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Segment ShallowCopy()
            {
                return (Segment)MemberwiseClone();
            }

#if DEBUG
            public int Depth
            {
                get
                {
                    int count = 1;
                    var prev = Previous;
                    while (prev != null)
                    {
                        count++;
                        prev = prev.Previous;
                    }
                    return count;
                }
            }

            public string DebugInfo => Encoding.UTF8.GetString(Address, Used);
#endif
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

        public UnmanagedWriteBuffer(JsonOperationContext context, AllocatedMemoryData allocatedMemoryData)
        {
            Debug.Assert(context != null);
            Debug.Assert(allocatedMemoryData != null);

            _context = context;
            _head = new Segment
            {
                Previous = null,
                DeallocationPendingPrevious = null,
                Allocation = allocatedMemoryData,
                Address = allocatedMemoryData.Address,
                Used = 0,
                AccumulatedSizeInBytes = 0
            };

#if MEM_GUARD
            AllocatedBy = Environment.StackTrace;
            FreedBy = null;
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(byte[] buffer, int start, int count)
        {
            Debug.Assert(start >= 0 && start < buffer.Length); // start is an index
            Debug.Assert(count >= 0); // count is a size
            Debug.Assert(start + count <= buffer.Length); // can't overrun the buffer

            fixed (byte* bufferPtr = buffer)
            {
                Debug.Assert(bufferPtr + start >= bufferPtr); // overflow check
                Write(bufferPtr + start, count);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowOnDisposed()
        {
#if DEBUG
            // PERF: This check will only happen in debug mode because it will fail with a NRE anyways on release.
            if (IsDisposed)
                throw new ObjectDisposedException(nameof(UnmanagedWriteBuffer));
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(byte* buffer, int count)
        {
            
            Debug.Assert(count >= 0); // count is a size
            Debug.Assert(buffer + count >= buffer); // overflow check
            ThrowOnDisposed();

            if (count == 0)
                return;

            var head = _head;
            if (head.Allocation.SizeInBytes - head.Used > count)
            {
               //Unsafe.CopyBlock(head.Address + head.Used, buffer, (uint)count);
                Memory.Copy(head.Address + head.Used, buffer, count);
                head.AccumulatedSizeInBytes += count;
                head.Used += count;
            }
            else
            {
                throw new NotSupportedException();
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte data)
        {
            throw new NotSupportedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            ThrowOnDisposed();

            _head.Used = 0;
            _head.AccumulatedSizeInBytes = 0;
            _head.Previous = null;
        }

        public void Dispose()
        {
            if (IsDisposed)
                return;

#if MEM_GUARD
            FreedBy = Environment.StackTrace;
#endif

            // The actual lifetime of the _head Segment is unbounded: it will
            // be released by the GC when we no longer have any references to
            // it (i.e. no more copies of this struct)
            //
            // We can, however, force the deallocation of all the previous
            // Segments by ensuring we don't keep any references after the
            // Dispose is run.

            var head = _head;
            _head = null; // Further references are NREs.
            for (Segment next; head != null; head = next)
            {
                _context.ReturnMemory(head.Allocation);

                // This is used to signal that Dispose has run to other copies
                head.Address = null;

#if DEBUG
                // Helps to avoid program errors, albeit unnecessary
                head.Allocation = null;
                head.AccumulatedSizeInBytes = -1;
                head.Used = -1;
#endif

                // `next` is used to keep a reference to the previous Segment.
                // Since `next` lives only within this for loop and we clear up
                // all other references, non-head Segments should be GC'd.
                next = head.DeallocationPendingPrevious;
                head.Previous = null;
                head.DeallocationPendingPrevious = null;
            }
        }

#if MEM_GUARD
        public string AllocatedBy;
        public string FreedBy;
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void EnsureSingleChunk(JsonParserState state)
        {
            
            EnsureSingleChunk(out state.StringBuffer, out state.StringSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void EnsureSingleChunk(out byte* ptr, out int size)
        {
            ThrowOnDisposed();
       

            if (_head.Previous == null)
            {
                // Common case is we have a single chunk already, so no need 
                // to do anything

               
                ptr = _head.Address;
                size = SizeInBytes;
                return;
            }
            
            throw new NotSupportedException();
        }

    }
}