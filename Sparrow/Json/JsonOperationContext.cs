using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;

#if VALIDATE
using Sparrow.Platform;
#endif

namespace Sparrow.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public class JsonOperationContext : IDisposable
    {
        private int _generation;
        private readonly int _initialSize;
        private readonly int _longLivedSize;
        private readonly ArenaMemoryAllocator _arenaAllocator;
        private ArenaMemoryAllocator _arenaAllocatorForLongLivedValues;
        private AllocatedMemoryData _tempBuffer;

        private readonly Dictionary<string, LazyStringValue> _fieldNames = new Dictionary<string, LazyStringValue>();


        private int _numberOfAllocatedStringsValues;
        private readonly List<LazyStringValue> _allocateStringValues = new List<LazyStringValue>(256);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe LazyStringValue AllocateStringValue(string str, byte* ptr, int size)
        {
            if (_numberOfAllocatedStringsValues < _allocateStringValues.Count)
            {
                var lazyStringValue = _allocateStringValues[_numberOfAllocatedStringsValues++];
                lazyStringValue.Renew(str, ptr, size);
                return lazyStringValue;
            }

            var allocateStringValue = new LazyStringValue(str, ptr, size, this);
            if (_numberOfAllocatedStringsValues < 25 * 1000)
            {
                _allocateStringValues.Add(allocateStringValue);
                _numberOfAllocatedStringsValues++;
            }
            return allocateStringValue;
        }             

        private readonly JsonParserState _jsonParserState;
        private readonly BlittableJsonDocumentBuilder _documentBuilder;

        public int Generation => _generation;

        public static JsonOperationContext ShortTermSingleUse()
        {
            return new JsonOperationContext(4096, 1024);
        }

        public JsonOperationContext(int initialSize, int longLivedSize)
        {

            _initialSize = initialSize;
            _longLivedSize = longLivedSize;
            _arenaAllocator = new ArenaMemoryAllocator();
            _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator();
            _jsonParserState = new JsonParserState();
            _documentBuilder = new BlittableJsonDocumentBuilder(this, _jsonParserState, null);

#if MEM_GUARD_STACK
            ElectricFencedMemory.IncrementConext();
            ElectricFencedMemory.RegisterContextAllocation(this,Environment.StackTrace);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetMemory(int requestedSize)
        {
#if DEBUG || VALIDATE
            if (requestedSize <= 0)
                throw new ArgumentException(nameof(requestedSize));
#endif

            var allocatedMemory = _arenaAllocator.Allocate(requestedSize);
            allocatedMemory.ContextGeneration = Generation;
            allocatedMemory.Parent = this;
            return allocatedMemory;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetLongLivedMemory(int requestedSize)
        {
#if DEBUG || VALIDATE
            if (requestedSize <= 0)
                throw new ArgumentException(nameof(requestedSize));
#endif
            //we should use JsonOperationContext in single thread
            if (_arenaAllocatorForLongLivedValues == null)
            {
                //_arenaAllocatorForLongLivedValues == null when the context is after Reset() but before Renew()
                ThrowAlreadyDisposedForLongLivedAllocator();

                //make compiler happy, previous row will throw
                return null;
            }

            var allocatedMemory = _arenaAllocatorForLongLivedValues.Allocate(requestedSize);
            allocatedMemory.ContextGeneration = Generation;
            allocatedMemory.Parent = this;
            return allocatedMemory;
        }

        private void ThrowAlreadyDisposedForLongLivedAllocator()
        {
            throw new ObjectDisposedException("Could not allocated long lived memory, because the context is after Reset() but before Renew(). Is it possible that you have tried to use the context AFTER it was returned to the context pool?");
        }

    
        public  void Dispose()
        {
            Disposed = true;
            Reset(true);

            _documentBuilder.Dispose();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue GetLazyStringForFieldWithCaching(string field)
        {
            EnsureNotDisposed();
            if (_fieldNames.TryGetValue(field, out LazyStringValue value))
            {
                // PERF: This is usually the most common scenario, so actually being contiguous improves the behavior.
                Debug.Assert(value.IsDisposed == false);
                return value;
            }

            return GetLazyStringForFieldWithCachingUnlikely(field);
        }

        private LazyStringValue GetLazyStringForFieldWithCachingUnlikely(string key)
        {
            EnsureNotDisposed();
            LazyStringValue value = GetLazyString(key, longLived: true);
            _fieldNames[key] = value;

            //sanity check, in case the 'value' is manually disposed outside of this function
            Debug.Assert(value.IsDisposed == false);
            return value;
        }

        private unsafe LazyStringValue GetLazyString(string field, bool longLived)
        {
            var state = new JsonParserState();
            var maxByteCount = Encoding.UTF8.GetMaxByteCount(field.Length);

            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(field);

            int memorySize = maxByteCount + escapePositionsSize;
            var memory = longLived ? GetLongLivedMemory(memorySize) : GetMemory(memorySize);

            fixed (char* pField = field)
            {
                var address = memory.Address;
                var actualSize = Encoding.UTF8.GetBytes(pField, field.Length, address, memory.SizeInBytes);

                state.FindEscapePositionsIn(address, actualSize, escapePositionsSize);

                state.WriteEscapePositionsTo(address + actualSize);
                LazyStringValue result = longLived == false ? AllocateStringValue(field, address, actualSize) : new LazyStringValue(field, address, actualSize, this);
                result.AllocatedMemoryData = memory;

                if (state.EscapePositions.Count > 0)
                {
                    Console.WriteLine( "Has escape positions!");
                    result.EscapePositions = state.EscapePositions.ToArray();
                }
                return result;
            }
        }            

        public bool Disposed;
        private void EnsureNotDisposed()
        {
            if (Disposed)
                ThrowObjectDisposed();
        }
        

        private void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException(nameof(JsonOperationContext));
        }

        protected internal virtual void Renew()
        {
            if (_arenaAllocatorForLongLivedValues == null)
            {
                _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator();
            }
        }

        protected internal virtual unsafe void Reset(bool forceReleaseLongLivedAllocator = false)
        {
            if (_tempBuffer != null && _tempBuffer.Address != null)
            {
                _arenaAllocator.Return(_tempBuffer);
                _tempBuffer = null;
            }

            _documentBuilder.Reset();

            // We don't reset _arenaAllocatorForLongLivedValues. It's used as a cache buffer for long lived strings like field names.
            // When a context is re-used, the buffer containing those field names was not reset and the strings are still valid and alive.

            var allocatorForLongLivedValues = _arenaAllocatorForLongLivedValues;
            if (allocatorForLongLivedValues != null || forceReleaseLongLivedAllocator)
            {
                foreach (var mem in _fieldNames.Values)
                {
                    _arenaAllocatorForLongLivedValues.Return(mem.AllocatedMemoryData);
                    mem.AllocatedMemoryData = null;
                    mem.Dispose();
                }

                _arenaAllocatorForLongLivedValues = null;
                _fieldNames.Clear();
            }
            _numberOfAllocatedStringsValues = 0;
            _generation = _generation + 1;

            if (_pooledArrays != null )
            {
                foreach (var pooledTypesKVP in _pooledArrays)
                {
                    foreach (var pooledArraysOfCurrentType in pooledTypesKVP.Value.Array)
                    {
                        pooledTypesKVP.Value.Releaser(pooledArraysOfCurrentType);
                    }
                }

                _pooledArrays = null;
            }
        }

        public void ReturnMemory(AllocatedMemoryData allocation)
        {
            EnsureNotDisposed();
            if (_generation != allocation.ContextGeneration)
                ThrowUseAfterFree(allocation);

            _arenaAllocator.Return(allocation);
        }

        private void ThrowUseAfterFree(AllocatedMemoryData allocation)
        {
#if MEM_GUARD_STACK || TRACK_ALLOCATED_MEMORY_DATA
            throw new InvalidOperationException(
                $"UseAfterFree detected! Attempt to return memory from previous generation, Reset has already been called and the memory reused! Allocated by: {allocation.AllocatedBy}. Thread name: {Thread.CurrentThread.Name}");
#else
            throw new InvalidOperationException(
                $"UseAfterFree detected! Attempt to return memory from previous generation, Reset has already been called and the memory reused! Thread name: {Thread.CurrentThread.Name}");
#endif
        }


        private Dictionary<Type, (Action<Array> Releaser, List<Array> Array)> _pooledArrays = null;

    }
}
