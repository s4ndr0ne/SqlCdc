namespace SqlCdc;

internal enum WatcherLifecycleState
{
    Idle = 0,
    Starting = 1,
    Running = 2,
    Stopping = 3,
    Disposed = 4,
}

internal sealed class WatcherLifecycleCoordinator
{
    private readonly object _sync = new();
    private WatcherLifecycleState _state = WatcherLifecycleState.Idle;

    public WatcherLifecycleState State
    {
        get
        {
            lock (_sync)
            {
                return _state;
            }
        }
    }

    public bool IsRunning => State is WatcherLifecycleState.Starting or WatcherLifecycleState.Running;

    public bool TryBeginStart()
    {
        lock (_sync)
        {
            if (_state is WatcherLifecycleState.Starting or WatcherLifecycleState.Running or WatcherLifecycleState.Stopping)
            {
                return false;
            }

            if (_state == WatcherLifecycleState.Disposed)
            {
                return false;
            }

            _state = WatcherLifecycleState.Starting;
            return true;
        }
    }

    public bool TryBeginStop()
    {
        lock (_sync)
        {
            if (_state == WatcherLifecycleState.Disposed)
            {
                return false;
            }

            _state = WatcherLifecycleState.Stopping;
            return true;
        }
    }

    public void CompleteStartFailure()
    {
        lock (_sync)
        {
            if (_state == WatcherLifecycleState.Starting)
            {
                _state = WatcherLifecycleState.Idle;
            }
        }
    }

    public void CompleteStop(bool disposed)
    {
        lock (_sync)
        {
            if (_state == WatcherLifecycleState.Disposed)
            {
                return;
            }

            _state = disposed ? WatcherLifecycleState.Disposed : WatcherLifecycleState.Idle;
        }
    }

    public void CompleteRunLoop(bool disposed)
    {
        lock (_sync)
        {
            if (_state == WatcherLifecycleState.Disposed)
            {
                return;
            }

            _state = disposed ? WatcherLifecycleState.Disposed : WatcherLifecycleState.Idle;
        }
    }

    public void TransitionTo(WatcherLifecycleState next)
    {
        lock (_sync)
        {
            if (_state == WatcherLifecycleState.Disposed && next != WatcherLifecycleState.Disposed)
            {
                return;
            }

            _state = next;
        }
    }
}
