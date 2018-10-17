using System;

namespace Sparrow.Platform
{
    public class IncorrectDllException : Exception
    {
        public IncorrectDllException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}