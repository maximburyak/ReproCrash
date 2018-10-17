using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    public struct Sorter<T, TSorter>
        where TSorter : struct, IComparer<T>
    {
        private readonly TSorter _sorter;        

        private const int SizeThreshold = 16;

        public Sorter(TSorter sorter)
        {
            _sorter = sorter;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Sort(T[] keys)
        {
            if (keys.Length < 2)
                return;

            IntroSort(keys, 0, keys.Length - 1, 2 * (int)Math.Floor(Math.Log(keys.Length)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Sort(T[] keys, int index, int length)
        {
            Debug.Assert(keys != null, "Check the arguments in the caller!");
            Debug.Assert(index >= 0 && length >= 0 && (keys.Length - index >= length), "Check the arguments in the caller!");
            if (length < 2)
                return;

            IntroSort(keys, index, length + index - 1, 2 * (int)Math.Floor(Math.Log(keys.Length)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SwapIfGreaterWithItems3(T[] keys, int a, int b, int c)
        {
            int x = a;
            int y = b == c ? b : c;
            int times = b == c ? 3 : -1;

            CompareLoop:
            bool swapped = false;
            times++;

            ref T ka = ref keys[x];
            ref T kb = ref keys[y];
            if (ka != null && _sorter.Compare(ka, kb) > 0)
            {
                T aux = ka;
                ka = kb;
                kb = aux;
                swapped = true;
            }

            if (times == 0)
            {
                y = b;
                goto CompareLoop;
            }
            if (times == 1 && !swapped)
            {
                x = b;
                y = c;
                goto CompareLoop;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Swap(T[] keys, int x, int y)
        {
            ref T kx = ref keys[x];
            ref T ky = ref keys[y];

            T aux = kx;
            kx = ky;
            ky = aux;
        }

        private void IntroSort(T[] keys, int lo, int hi, int depthLimit)
        {
            Contract.Requires(keys != null);
            Contract.Requires(lo >= 0);
            Contract.Requires(hi < keys.Length);

            int partitionSize;
            while (hi > lo)
            {
                partitionSize = hi - lo + 1;
                if (partitionSize <= SizeThreshold)
                    goto InsertionSortEnd;

                if (depthLimit == 0)
                    goto HeapSortEnd;

                depthLimit--;

                // Compute median-of-three.  But also partition them, since we've done the comparison.
                int middle = (hi + lo) / 2;

                // Sort lo, mid and hi appropriately, then pick mid as the pivot.
                SwapIfGreaterWithItems3(keys, lo, middle, hi);

                T pivot = keys[middle];
                int p;
                if (pivot == null) 
                {
                    p = PickPivotAndPartitionUnlikely(keys, lo, hi, middle);
                    goto PickPivotUnlikely;
                }

                Swap(keys, middle, hi - 1);

                int left = lo, right = hi - 1; // We already partitioned lo and hi and put the pivot in hi - 1.  And we pre-increment & decrement below.
                while (left < right)
                {
                    while (_sorter.Compare(pivot, keys[++left]) > 0) ;
                    while (_sorter.Compare(pivot, keys[--right]) < 0) ;

                    if (left >= right)
                        break;

                    Swap(keys, left, right);
                }

                // Put pivot in the right location.
                Swap(keys, left, hi - 1);

                p = left;

                PickPivotUnlikely:

                // Note we've already partitioned around the pivot and do not have to move the pivot again.
                IntroSort(keys, p + 1, hi, depthLimit);
                hi = p - 1;
            }

            return;

            InsertionSortEnd:
            {
                if (partitionSize >= 4) // Probabilistically the most likely case
                {
                    InsertionSort(keys, lo, hi);
                    goto Return; // We are done with this segment.
                }

                if (partitionSize >= 2)
                {
                    int mid = partitionSize == 3 ? hi - 1 : hi;
                    SwapIfGreaterWithItems3(keys, lo, mid, hi);
                }

                goto Return; // We are done with this segment.
            }

            HeapSortEnd:
            {
                HeapSort(keys, lo, hi);
            }

            Return:
            ;
        }

        private int PickPivotAndPartitionUnlikely(T[] keys, int lo, int hi, int middle)
        {
            Swap(keys, middle, hi - 1);

            int left = lo, right = hi - 1; // We already partitioned lo and hi and put the pivot in hi - 1.  And we pre-increment & decrement below.
            while (left < right)
            {
                while (left < (hi - 1) && keys[++left] == null) ;
                while (right > lo && keys[--right] != null) ;

                if (left >= right)
                    break;

                Swap(keys, left, right);
            }

            // Put pivot in the right location.
            Swap(keys, left, hi - 1);

            return left;
        }

        private void HeapSort(T[] keys, int lo, int hi)
        {
            Contract.Requires(keys != null);
            Contract.Requires(lo >= 0);
            Contract.Requires(hi > lo);
            Contract.Requires(hi < keys.Length);

            int n = hi - lo + 1;
            for (int i = n / 2; i >= 1; i--)
            {
                DownHeap(keys, i, n, lo);
            }
            for (int i = n; i > 1; i--)
            {
                Swap(keys, lo, lo + i - 1);
                DownHeap(keys, 1, i - 1, lo);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DownHeap(T[] keys, int i, int n, int lo)
        {
            Contract.Requires(keys != null);
            Contract.Requires(lo >= 0);
            Contract.Requires(lo < keys.Length);

            T d = keys[lo + i - 1];
            while (i <= n / 2)
            {
                int child = 2 * i;
                ref var key = ref keys[lo + child - 1];
                if (child < n && (key == null || _sorter.Compare(key, keys[lo + child]) < 0))
                {
                    child++;
                }

                ref var key2 = ref keys[lo + child - 1];
                if (key2 == null || _sorter.Compare(key2, d) < 0)
                    break;

                keys[lo + i - 1] = key2;
                i = child;
            }
            keys[lo + i - 1] = d;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InsertionSort(T[] keys, int lo, int hi)
        {
            Contract.Requires(keys != null);
            Contract.Requires(lo >= 0);
            Contract.Requires(hi >= lo);
            Contract.Requires(hi <= keys.Length);

            for (int i = lo; i < hi; i++)
            {
                int j = i;
                T t = keys[i + 1];

                while (j >= lo && (t == null || _sorter.Compare(t, keys[j]) < 0))
                {
                    keys[j + 1] = keys[j];
                    j--;
                }
                keys[j + 1] = t;
            }
        }
    }
}

