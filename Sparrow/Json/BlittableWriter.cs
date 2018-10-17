using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using static Sparrow.Json.BlittableJsonDocumentBuilder;

namespace Sparrow.Json
{
    public sealed class BlittableWriter<TWriter> : IDisposable
        where TWriter : struct
    {
        private readonly JsonOperationContext _context;
        //private TWriter _unmanagedWriteBuffer;
        private AllocatedMemoryData _compressionBuffer;
        private AllocatedMemoryData _innerBuffer;
        private int _position;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]

        public int WriteValue(long value)
        {
            var startPos = _position;
            _position += WriteVariableSizeLong(value);
            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(ulong value)
        {
            var s = value.ToString("G", CultureInfo.InvariantCulture);
            return WriteValue(s, out BlittableJsonToken token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(bool value)
        {
            var startPos = _position;
            _position += WriteVariableSizeInt(value ? 1 : 0);
            return startPos;
        }

        public int WriteValue(double value)
        {
            var s = EnsureDecimalPlace(value, value.ToString("R", CultureInfo.InvariantCulture));
            BlittableJsonToken token;
            return WriteValue(s, out token);
        }

        public int WriteValue(decimal value)
        {
            var s = EnsureDecimalPlace(value, value.ToString("G", CultureInfo.InvariantCulture));
            BlittableJsonToken token;
            return WriteValue(s, out token);
        }

        public int WriteValue(float value)
        {
            return WriteValue((double)value);
        }  

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(byte value)
        {
            var startPos = _position;
            //_unmanagedWriteBuffer.WriteByte(value);
            _position++;
            return startPos;
        }

        private static string EnsureDecimalPlace(double value, string text)
        {
            if (double.IsNaN(value) || double.IsInfinity(value) || double.IsNegativeInfinity(value) || text.IndexOf('.') != -1 || text.IndexOf('E') != -1 || text.IndexOf('e') != -1)
                return text;

            return text + ".0";
        }


        private static string EnsureDecimalPlace(decimal value, string text)
        {
            if (text.IndexOf('.') != -1)
                return text;

            return text + ".0";
        }   


        [ThreadStatic]
        private static List<int> _intBuffer;
        [ThreadStatic]
        private static int[] _propertyArrayOffset;

        static BlittableWriter()
        {
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteVariableSizeLong(long value)
        {
            // see zig zap trick here:
            // https://developers.google.com/protocol-buffers/docs/encoding?csw=1#types
            // for negative values

            var buffer = _innerBuffer.Address;
            var count = 0;
            var v = (ulong)((value << 1) ^ (value >> 63));
            while (v >= 0x80)
            {
                buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }
            buffer[count++] = (byte)(v);

            return count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteVariableSizeInt(int value)
        {
            // assume that we don't use negative values very often
            var buffer = _innerBuffer.Address;

            var count = 0;
            var v = (uint)value;
            while (v >= 0x80)
            {
                buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }
            buffer[count++] = (byte)(v);
            return count;
        }
    

        public unsafe int WriteValue(string str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
        {
            token = BlittableJsonToken.String;
            return 1;
        }

        public int WriteValue(LazyStringValue str)
        {
            return WriteValue(str, out _, UsageMode.None, null);
        }

        public unsafe int WriteValue(LazyStringValue str, out BlittableJsonToken token,
            UsageMode mode, int? initialCompressedSize)
        {
            token = BlittableJsonToken.String;
            return 1;
        }
        public unsafe int WriteValue(byte* buffer, int size, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
        {
            int startPos = _position;
            token = BlittableJsonToken.String;

            _position += WriteVariableSizeInt(size);

            // if we are more than this size, we want to abort the compression early and just use
            // the verbatim string
            int maxGoodCompressionSize = size - sizeof(int) * 2;
            if (maxGoodCompressionSize > 0)
            {
                size = TryCompressValue(ref buffer, ref _position, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
            }

            //_unmanagedWriteBuffer.Write(buffer, size);
            _position += size;

            _position += WriteVariableSizeInt(0);
            return startPos;
        }

        public unsafe int WriteValue(byte* buffer, int size, List<int> escapePositions, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
        {
            int position = _position;

            int startPos = position;
            token = BlittableJsonToken.String;

            position += WriteVariableSizeInt(size);

            // if we are more than this size, we want to abort the compression early and just use
            // the verbatim string
            int maxGoodCompressionSize = size - sizeof(int) * 2;
            if (maxGoodCompressionSize > 0)
            {
                size = TryCompressValue(ref buffer, ref position, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
            }

            //_unmanagedWriteBuffer.Write(buffer, size);
            position += size;

            if (escapePositions == null)
            {
                position += WriteVariableSizeInt(0);
                goto Finish;
            }

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            position += WriteVariableSizeInt(escapePositions.Count);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach. 
            int count = escapePositions.Count;
            for (int i = 0; i < count; i++)
                position += WriteVariableSizeInt(escapePositions[i]);

            Finish:
            _position = position;
            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteValue(byte* buffer, int size)
        {
            int startPos = _position;

            int writtenBytes = WriteVariableSizeInt(size);
           // _unmanagedWriteBuffer.Write(buffer, size);
            writtenBytes += size;
            writtenBytes += WriteVariableSizeInt(0);

            _position += writtenBytes;

            return startPos;
        }

        public unsafe int WriteValue(byte* buffer, int size, int[] escapePositions)
        {
            var startPos = _position;
            _position += WriteVariableSizeInt(size);
          //  _unmanagedWriteBuffer.Write(buffer, size);
            _position += size;

            if (escapePositions == null || escapePositions.Length == 0)
            {
                _position += WriteVariableSizeInt(0);
                return startPos;
            }

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            _position += WriteVariableSizeInt(escapePositions.Length);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach. 
            int count = escapePositions.Length;
            for (int i = 0; i < count; i++)
                _position += WriteVariableSizeInt(escapePositions[i]);

            return startPos;
        }

        public unsafe int WriteValue(byte* buffer, int size, List<int> escapePositions)
        {
            int position = _position;

            int startPos = position;
            position += WriteVariableSizeInt(size);
          //  _unmanagedWriteBuffer.Write(buffer, size);
            position += size;

            if (escapePositions == null || escapePositions.Count == 0)
            {
                position += WriteVariableSizeInt(0);
                goto Finish;
            }

            int escapePositionCount = escapePositions.Count;

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            position += WriteVariableSizeInt(escapePositionCount);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach. 
            for (int i = 0; i < escapePositionCount; i++)
                position += WriteVariableSizeInt(escapePositions[i]);

            Finish:
            _position = position;
            return startPos;
        }

        public unsafe int WriteValue(byte* buffer, int size, int[] escapePositions, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
        {
            var startPos = _position;
            token = BlittableJsonToken.String;

            _position += WriteVariableSizeInt(size);

            // if we are more than this size, we want to abort the compression early and just use
            // the verbatim string
            int maxGoodCompressionSize = size - sizeof(int) * 2;
            if (maxGoodCompressionSize > 0)
            {
                size = TryCompressValue(ref buffer, ref _position, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
            }

            //_unmanagedWriteBuffer.Write(buffer, size);
            _position += size;

            if (escapePositions == null)
            {
                _position += WriteVariableSizeInt(0);
                return startPos;
            }

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            _position += WriteVariableSizeInt(escapePositions.Length);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach. 
            int count = escapePositions.Length;
            for (int i = 0; i < count; i++)
                _position += WriteVariableSizeInt(escapePositions[i]);

            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int TryCompressValue(ref byte* buffer, ref int position, int size, ref BlittableJsonToken token, UsageMode mode, int? initialCompressedSize, int maxGoodCompressionSize)
        {
               
            return size;
        }


        public void Dispose()
        {
        //    _unmanagedWriteBuffer.Dispose();
            if (_compressionBuffer != null)
                _context.ReturnMemory(_compressionBuffer);

            _compressionBuffer = null;

            if (_innerBuffer != null)
                _context.ReturnMemory(_innerBuffer);
            _innerBuffer = null;
        }
    }
}
