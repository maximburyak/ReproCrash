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
using Sparrow.Threading;

namespace Sparrow.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public class JsonOperationContext : IDisposable
    {
        private int _generation;                
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

            var allocateStringValue = new LazyStringValue(str, ptr, size);
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
            return new JsonOperationContext();
        }

        public JsonOperationContext()
        {            
            _disposeOnceRunner = new DisposeOnce<ExceptionRetry>(() =>
            {
                Reset(true);

                _documentBuilder.Dispose();                
            });                        
            
            _jsonParserState = new JsonParserState();
            _documentBuilder = new BlittableJsonDocumentBuilder(this, _jsonParserState, null);            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetMemory(int requestedSize)
        {
            var allocatedMemory = new AllocatedMemoryData(requestedSize);
            allocatedMemory.ContextGeneration = Generation;
            allocatedMemory.Parent = this;

            return allocatedMemory;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetLongLivedMemory(int requestedSize)
        {
            var allocatedMemory = new AllocatedMemoryData(requestedSize);
            allocatedMemory.ContextGeneration = Generation;
            allocatedMemory.Parent = this;
            return allocatedMemory;
        }
    
        private readonly DisposeOnce<ExceptionRetry> _disposeOnceRunner;
        private bool Disposed => _disposeOnceRunner.Disposed;
        public  void Dispose()
        {
            _disposeOnceRunner.Dispose();
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

            return GetLazyString(field, longLived: true);
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
                LazyStringValue result = longLived == false ? AllocateStringValue(field, address, actualSize) : new LazyStringValue(field, address, actualSize);
                result.AllocatedMemoryData = memory;

                if (state.EscapePositions.Count > 0)
                {
                    Console.WriteLine( "Has escape positions!");
                    result.EscapePositions = state.EscapePositions.ToArray();
                }
                return result;
            }
        }      

        private void EnsureNotDisposed()
        {
            if (Disposed)
                ThrowObjectDisposed();
        }
        private void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException(nameof(JsonOperationContext));
        }

        protected internal virtual unsafe void Reset(bool forceReleaseLongLivedAllocator = false)
        {
            _documentBuilder.Reset();

            // We don't reset _arenaAllocatorForLongLivedValues. It's used as a cache buffer for long lived strings like field names.
            // When a context is re-used, the buffer containing those field names was not reset and the strings are still valid and alive.
           
            foreach (var mem in _fieldNames.Values)
            {
                mem.AllocatedMemoryData = null;
                mem.Dispose();
            }
            

            _fieldNames.Clear();
                        
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
        
        private Dictionary<Type, (Action<Array> Releaser, List<Array> Array)> _pooledArrays = null;
    }
}
