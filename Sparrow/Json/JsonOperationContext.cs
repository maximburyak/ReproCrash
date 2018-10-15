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
        public const int InitialStreamSize = 4096;
        private const int MaxInitialStreamSize = 16 * 1024 * 1024;
        private readonly int _initialSize;
        private readonly int _longLivedSize;
        private readonly ArenaMemoryAllocator _arenaAllocator;
        private ArenaMemoryAllocator _arenaAllocatorForLongLivedValues;
        private AllocatedMemoryData _tempBuffer;
        private List<string> _normalNumbersStringBuffers = new List<string>(5);
        private string _hugeNumbersBuffer;

        private readonly Dictionary<string, LazyStringValue> _fieldNames = new Dictionary<string, LazyStringValue>();

        private struct PathCacheHolder
        {
            public PathCacheHolder(Dictionary<StringSegment, object> path, Dictionary<int, object> byIndex)
            {
                Path = path;
                ByIndex = byIndex;
            }

            public readonly Dictionary<StringSegment, object> Path;
            public readonly Dictionary<int, object> ByIndex;
        }

        private int _numberOfAllocatedPathCaches = -1;
        private readonly PathCacheHolder[] _allocatePathCaches = new PathCacheHolder[512];
        private Stack<MemoryStream> _cachedMemoryStreams = new Stack<MemoryStream>();

        private int _numberOfAllocatedStringsValues;
        private readonly List<LazyStringValue> _allocateStringValues = new List<LazyStringValue>(256);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AcquirePathCache(out Dictionary<StringSegment, object> pathCache, out Dictionary<int, object> pathCacheByIndex)
        {
            // PERF: Avoids allocating gigabytes in FastDictionary instances on high traffic RW operations like indexing. 
            if (_numberOfAllocatedPathCaches >= 0)
            {
                var cache = _allocatePathCaches[_numberOfAllocatedPathCaches--];
                Debug.Assert(cache.Path != null);
                Debug.Assert(cache.ByIndex != null);

                pathCache = cache.Path;
                pathCacheByIndex = cache.ByIndex;

                return;
            }

            pathCache = new Dictionary<StringSegment, object>();
            pathCacheByIndex = new Dictionary<int, object>();
        }

        public void ReleasePathCache(Dictionary<StringSegment, object> pathCache, Dictionary<int, object> pathCacheByIndex)
        {
            if (_numberOfAllocatedPathCaches < _allocatePathCaches.Length - 1 && pathCache.Count < 256)
            {
                pathCache.Clear();
                pathCacheByIndex.Clear();

                _allocatePathCaches[++_numberOfAllocatedPathCaches] = new PathCacheHolder(pathCache, pathCacheByIndex);
            }
        }

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
       
        internal unsafe class BufferSegment
        {
            public byte[] Array;
            public int Offset;
            public int Count;
            public byte* Ptr;
        }

        
        public CachedProperties CachedProperties;

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
            CachedProperties = new CachedProperties(this);
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
        public LazyStringValue GetLazyStringForFieldWithCaching(StringSegment key)
        {
            EnsureNotDisposed();

            var field = key.Value; // This will allocate if we are using a substring. 
            if (_fieldNames.TryGetValue(field, out LazyStringValue value))
            {
                //sanity check, in case the 'value' is manually disposed outside of this function
                Debug.Assert(value.IsDisposed == false);
                return value;
            }

            return GetLazyStringForFieldWithCachingUnlikely(field);
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

        private LazyStringValue GetLazyStringForFieldWithCachingUnlikely(StringSegment key)
        {
            EnsureNotDisposed();
            LazyStringValue value = GetLazyString(key, longLived: true);
            _fieldNames[key] = value;

            //sanity check, in case the 'value' is manually disposed outside of this function
            Debug.Assert(value.IsDisposed == false);
            return value;
        }

        public LazyStringValue GetLazyString(string field)
        {
            EnsureNotDisposed();

            if (field == null)
                return null;

            return GetLazyString(field, longLived: false);
        }

        
        private unsafe LazyStringValue GetLazyString(StringSegment field, bool longLived)
        {
            var state = new JsonParserState();
            var maxByteCount = Encoding.UTF8.GetMaxByteCount(field.Length);

            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(field);

            int memorySize = maxByteCount + escapePositionsSize;
            var memory = longLived ? GetLongLivedMemory(memorySize) : GetMemory(memorySize);

            fixed (char* pField = field.Buffer)
            {
                var address = memory.Address;
                var actualSize = Encoding.UTF8.GetBytes(pField + field.Offset, field.Length, address, memory.SizeInBytes);

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

        private BlittableJsonReaderObject ParseToMemory(Stream stream, string debugTag, BlittableJsonDocumentBuilder.UsageMode mode)
        {
            var bytes = new byte[1024 * 64];
            return ParseToMemory(stream, debugTag, mode, (bytes) );
        }

        public unsafe BlittableJsonReaderObject ParseToMemory(Stream stream, string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode,
            byte[] bytes)
        {

            EnsureNotDisposed();
            int used =0, valid = 0;
            _jsonParserState.Reset();
            fixed(byte* buffer = bytes)
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState))
            {
                CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                while (true)
                {
                    if (valid == used)
                    {
                        var read = stream.Read(bytes, 0, bytes.Length);
                        EnsureNotDisposed();
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        valid = read;
                        used = 0;
                    }
                    parser.SetBuffer(buffer, valid);
                    var result = builder.Read();
                    used += parser.BufferOffset;
                    if (result)
                        break;
                }
                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
        }

        public unsafe BlittableJsonReaderObject ParseBuffer(byte* buffer, int length, string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode)
        {

            EnsureNotDisposed();

            _jsonParserState.Reset();
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState))
            {
                CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                parser.SetBuffer(buffer, length);

                if (builder.Read() == false)
                    throw new EndOfStreamException("Buffer ended without reaching end of json content");

                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
        }

        public bool Disposed;
        private void EnsureNotDisposed()
        {
            if (Disposed)
                ThrowObjectDisposed();
        }
        
        private void DisposeIfNeeded(int generation, UnmanagedJsonParser parser, BlittableJsonDocumentBuilder builder)
        {
            // if the generation has changed, that means that we had reset the context
            // this can happen if we were waiting on an async call for a while, got timed out / error / something
            // and the context was reset before we got back from the async call
            // since the full context was reset, there is no point in trying to dispose things, they were already 
            // taken care of
            if (generation == _generation)
            {
                parser?.Dispose();
                builder?.Dispose();
            }
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
                CachedProperties = new CachedProperties(this);
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
                CachedProperties = null; // need to release this so can be collected
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

        public void Write(Stream stream, BlittableJsonReaderObject json)
        {
            EnsureNotDisposed();
           
        }


        public unsafe double ParseDouble(byte* ptr, int length)
        {
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return double.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseDouble(byte* ptr, int length, out double val)
        {
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);
                        
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return double.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture,out val);            
        }

        public unsafe decimal ParseDecimal(byte* ptr, int length)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return decimal.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseDecimal(byte* ptr, int length, out decimal val)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return decimal.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture, out val);
        }

        public unsafe float ParseFloat(byte* ptr, int length)
        {
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return float.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseFloat(byte* ptr, int length, out float val)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return float.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture, out val);
        }

        public unsafe long ParseLong(byte* ptr, int length)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return long.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseLong(byte* ptr, int length, out long val)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return long.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture, out val);
        }

        public unsafe ulong ParseULong(byte* ptr, int length)
        {
            EnsureNotDisposed();
            var stringBuffer= InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return ulong.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseULong(byte* ptr, int length, out ulong val)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return ulong.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture, out val);
        }       

        private unsafe string InitializeStringBufferForNumberParsing(byte* ptr, int length)
        {
            var lengthsNextPowerOf2 = (length);

            var actualPowerOf2 = (int)Math.Pow(lengthsNextPowerOf2, 0.5);
            string stringBuffer;
            if (actualPowerOf2 <= _normalNumbersStringBuffers.Count)
            {
                stringBuffer = _normalNumbersStringBuffers[actualPowerOf2 - 1];

                if (stringBuffer == null)
                {
                    stringBuffer = _normalNumbersStringBuffers[actualPowerOf2 - 1] = new string(' ', lengthsNextPowerOf2);
                }
            }
            else
            {
                stringBuffer = _hugeNumbersBuffer;
                if (_hugeNumbersBuffer == null || length > _hugeNumbersBuffer.Length)
                    stringBuffer = _hugeNumbersBuffer = new string(' ', length);
            }
            // we should support any length of LazyNumber, therefore, we do not validate it's length
            
            
            // here we assume a clear char <- -> byte conversion, we only support
            // utf8, and those cleanly transfer
            fixed (char* pChars = stringBuffer)
            {
                int i = 0;

                for (; i < length; i++)
                {
                    pChars[i] = (char)ptr[i];
                }
                for (; i < stringBuffer.Length; i++)
                {
                    pChars[i] = ' ';
                }
            }

            return stringBuffer;
        }

        public MemoryStream CheckoutMemoryStream()
        {
            EnsureNotDisposed();
            if (_cachedMemoryStreams.Count == 0)
            {
                return new MemoryStream();
            }

            return _cachedMemoryStreams.Pop();
        }

        public void ReturnMemoryStream(MemoryStream stream)
        {
            EnsureNotDisposed();
            stream.SetLength(0);
            _cachedMemoryStreams.Push(stream);
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
