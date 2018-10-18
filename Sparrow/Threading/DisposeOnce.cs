using System;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Threading
{
    public interface IDisposeOnceOperationMode {}
    public struct ExceptionRetry : IDisposeOnceOperationMode { }
    
    public sealed class DisposeOnce        
    {
        private readonly Action _action;
        private Tuple<MultipleUseFlag, TaskCompletionSource<object>> _state 
            = Tuple.Create(new MultipleUseFlag(), new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));

        public DisposeOnce(Action action)
        {
            _action = action;            
        }

        /// <summary>
        /// Runs the dispose action. Ensures any threads that are running it
        /// concurrently wait for the dispose to finish if it is in progress.    
        /// </summary>
        public void Dispose()
        {
            var localState = _state;
            var disposeInProgress = localState.Item1;
            if (disposeInProgress.Raise() == false)
            {
                // If a dispose is in progress, all other threads
                // attempting to dispose will stop here and wait until it
                // is over. This call to Wait may throw with an
                // AggregateException
                localState.Item2.Task.Wait();
                return;
            }

            try
            {
                _action();

                // Let everyone know this run worked out!
                localState.Item2.SetResult(null);
            }
            catch (Exception e)
            {                
                // Reset the state for the next attempt. First backup the
                // current task completion.
                // Let everyone waiting know that this run failed
                localState.Item2.SetException(e);

                // atomically replace both the flag and the task to wait, so new 
                // callers to the Dispose are either getting the error or can start
                // calling this again
                Interlocked.CompareExchange(ref _state,
                    Tuple.Create(new MultipleUseFlag(), new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously)),
                    localState
                );

                throw;
            }
        }

        public bool Disposed
        {
            get
            {
                var state = _state;
                if (state.Item1 == false)
                    return false;                
                
                if (state.Item2.Task.IsFaulted || state.Item2.Task.IsCanceled)
                    return false;

                return state.Item2.Task.IsCompleted;
                                
            }
        }
    }    
}
