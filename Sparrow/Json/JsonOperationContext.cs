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

namespace Sparrow.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public class JsonOperationContext : IDisposable
    {        
        private readonly ArenaMemoryAllocator _arenaAllocator;
        private ArenaMemoryAllocator _arenaAllocatorForLongLivedValues;

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

        public static JsonOperationContext ShortTermSingleUse()
        {
            return new JsonOperationContext();
        }

        public JsonOperationContext()
        {            
            _arenaAllocator = new ArenaMemoryAllocator();
            _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator();
            _jsonParserState = new JsonParserState();
            _documentBuilder = new BlittableJsonDocumentBuilder(_jsonParserState, null);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetMemory(int requestedSize)
        {
            var allocatedMemory = _arenaAllocator.Allocate(requestedSize);
            allocatedMemory.Parent = this;
            return allocatedMemory;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetLongLivedMemory(int requestedSize)
        {
            //we should use JsonOperationContext in single thread
            if (_arenaAllocatorForLongLivedValues == null)
            {
                //_arenaAllocatorForLongLivedValues == null when the context is after Reset() but before Renew()
                ThrowAlreadyDisposedForLongLivedAllocator();

                //make compiler happy, previous row will throw
                return null;
            }

            var allocatedMemory = _arenaAllocatorForLongLivedValues.Allocate(requestedSize);
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
            return GetLazyStringForFieldWithCachingUnlikely(field);
        }

        private LazyStringValue GetLazyStringForFieldWithCachingUnlikely(string key)
        {
            LazyStringValue value = GetLazyString(key, longLived: true);

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
             
        protected internal virtual unsafe void Reset(bool forceReleaseLongLivedAllocator = false)
        {
            _documentBuilder.Reset();

            // We don't reset _arenaAllocatorForLongLivedValues. It's used as a cache buffer for long lived strings like field names.
            // When a context is re-used, the buffer containing those field names was not reset and the strings are still valid and alive.

            var allocatorForLongLivedValues = _arenaAllocatorForLongLivedValues;
            if (allocatorForLongLivedValues != null || forceReleaseLongLivedAllocator)
            {            
                _arenaAllocatorForLongLivedValues = null;
            }
            _numberOfAllocatedStringsValues = 0;

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
            _arenaAllocator.Return(allocation);
        }

        private Dictionary<Type, (Action<Array> Releaser, List<Array> Array)> _pooledArrays = null;

    }
}
