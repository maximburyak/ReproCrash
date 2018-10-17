using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sparrow
{
 
    public struct StringSegment : IEquatable<StringSegment>
    {
        public readonly string Buffer;
        public readonly int Length;
        public readonly int Offset;

        private string _valueString;
        public string Value => _valueString ?? (_valueString = Buffer.Substring(Offset, Length));


        // PERF: Included this version to exploit the knowledge that we are going to get a full string.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment(string buffer)
        {
            Offset = 0;
            Length = buffer?.Length ?? 0;
            Buffer = buffer;
            _valueString = buffer;
        }

 
        // String's indexing will throw a IndexOutOfRange exception if required
        public char this[int index]
        {
            get
            {
                Debug.Assert(index >= 0 && index < Length);
                return Buffer[Offset + index];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator StringSegment(string buffer)
        {
            return new StringSegment(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator string(StringSegment segment)
        {
            return segment.Value;
        }      

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is StringSegment && Equals((StringSegment)obj);
        }

        public override unsafe int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public unsafe bool Equals(string other)
        {
            if (other == null)
                return Buffer == null;

            if (Length != other.Length)
                return false;

            fixed (char* pSelf = Buffer)
            fixed (char* pOther = other)
            {
                return Memory.Compare((byte*)pSelf + Offset * sizeof(char), (byte*)pOther, Length * sizeof(char)) == 0;
            }
        }

        public bool Equals(string other, StringComparison stringComparison)
        {
            if (other == null)
                return Buffer == null;

            if (Length != other.Length)
                return false;
            return string.Compare(Buffer, Offset, other, 0, Length, stringComparison) == 0;
        }

        public unsafe bool Equals(StringSegment other)
        {
            if (Length != other.Length)
                return false;

            fixed (char* pSelf = Buffer)
            fixed (char* pOther = other.Buffer)
            {
                return Memory.Compare((byte*)pSelf + Offset * sizeof(char), (byte*)pOther + other.Offset * sizeof(char), Length * sizeof(char)) == 0;
            }
        }

        public bool Equals(StringSegment other, StringComparison stringComparison)
        {
            if (Length != other.Length)
                return false;
            return string.Compare(Buffer, Offset, other.Buffer, other.Offset, Length, stringComparison) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string ToString()
        {
            return Value;
        }
    }
}
