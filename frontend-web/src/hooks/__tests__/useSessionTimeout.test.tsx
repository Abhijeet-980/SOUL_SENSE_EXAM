/**
 * @jest-environment jsdom
 * 
 * useSessionTimeout Hook Tests (Issue #999)
 * -----------------------------------------
 * Tests for session timeout hook functionality.
 */

import React from 'react';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useSessionTimeout } from '../useSessionTimeout';

// Mock useAuth hook
const mockLogout = jest.fn();
const mockIsAuthenticated = true;

jest.mock('../useAuth', () => ({
  useAuth: () => ({
    logout: mockLogout,
    isAuthenticated: mockIsAuthenticated,
  }),
}));

// Mock toast
const mockToastError = jest.fn();
const mockToastSuccess = jest.fn();

jest.mock('@/lib/toast', () => ({
  toast: {
    error: mockToastError,
    success: mockToastSuccess,
  },
}));

// Mock next/navigation
const mockPush = jest.fn();

jest.mock('next/navigation', () => ({
  useRouter: () => ({
    push: mockPush,
  }),
  usePathname: () => '/dashboard',
}));

describe('useSessionTimeout Hook', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
    
    // Clear any event listeners
    window.removeEventListener = jest.fn();
    window.addEventListener = jest.fn();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('Basic Functionality', () => {
    it('should initialize with warning hidden', () => {
      const { result } = renderHook(() => useSessionTimeout({ enabled: true }));

      expect(result.current.showWarning).toBe(false);
      expect(result.current.remainingSeconds).toBe(0);
    });

    it('should not start timer when disabled', () => {
      renderHook(() => useSessionTimeout({ enabled: false }));

      jest.advanceTimersByTime(15 * 60 * 1000); // 15 minutes
      expect(mockLogout).not.toHaveBeenCalled();
    });

    it('should not start timer when not authenticated', () => {
      // Override mock to return not authenticated
      jest.mocked(require('../useAuth').useAuth).mockReturnValue({
        logout: mockLogout,
        isAuthenticated: false,
      });

      renderHook(() => useSessionTimeout({ enabled: true }));

      jest.advanceTimersByTime(15 * 60 * 1000);
      expect(mockLogout).not.toHaveBeenCalled();
    });
  });

  describe('Timeout Detection', () => {
    it('should call logout after inactivity timeout', () => {
      renderHook(() => useSessionTimeout({ enabled: true }));

      // Advance to 15 minutes
      act(() => {
        jest.advanceTimersByTime(15 * 60 * 1000);
      });

      expect(mockLogout).toHaveBeenCalledTimes(1);
      expect(mockToastError).toHaveBeenCalledWith(
        'Your session has expired due to inactivity. Please log in again.'
      );
    });

    it('should show warning 30 seconds before timeout', () => {
      const { result } = renderHook(() => useSessionTimeout({ enabled: true }));

      // Advance to 14 minutes 30 seconds (30 seconds before timeout)
      act(() => {
        jest.advanceTimersByTime(14 * 60 * 1000 + 30 * 1000);
      });

      expect(result.current.showWarning).toBe(true);
      expect(result.current.remainingSeconds).toBe(30);
    });

    it('should countdown warning seconds', () => {
      const { result } = renderHook(() => useSessionTimeout({ enabled: true }));

      // Advance to warning threshold
      act(() => {
        jest.advanceTimersByTime(14 * 60 * 1000 + 30 * 1000);
      });

      expect(result.current.remainingSeconds).toBe(30);

      // Advance 5 seconds
      act(() => {
        jest.advanceTimersByTime(5000);
      });

      expect(result.current.remainingSeconds).toBe(25);
    });
  });

  describe('Activity Reset', () => {
    it('should reset timer on user activity', () => {
      renderHook(() => useSessionTimeout({ enabled: true }));

      // Advance 10 minutes
      act(() => {
        jest.advanceTimersByTime(10 * 60 * 1000);
      });

      // Simulate activity
      act(() => {
        window.dispatchEvent(new Event('mousedown'));
      });

      // Advance another 10 minutes (total 20 from start, but only 10 since activity)
      act(() => {
        jest.advanceTimersByTime(10 * 60 * 1000);
      });

      // Should not have logged out yet
      expect(mockLogout).not.toHaveBeenCalled();

      // Now advance to full 15 minutes from last activity
      act(() => {
        jest.advanceTimersByTime(5 * 60 * 1000);
      });

      expect(mockLogout).toHaveBeenCalled();
    });

    it('should reset warning on activity', () => {
      const { result } = renderHook(() => useSessionTimeout({ enabled: true }));

      // Advance to warning
      act(() => {
        jest.advanceTimersByTime(14 * 60 * 1000 + 30 * 1000);
      });

      expect(result.current.showWarning).toBe(true);

      // Simulate activity
      act(() => {
        window.dispatchEvent(new Event('keydown'));
      });

      expect(result.current.showWarning).toBe(false);
    });
  });

  describe('continueSession', () => {
    it('should hide warning and reset timer', () => {
      const { result } = renderHook(() => useSessionTimeout({ enabled: true }));

      // Advance to warning
      act(() => {
        jest.advanceTimersByTime(14 * 60 * 1000 + 30 * 1000);
      });

      expect(result.current.showWarning).toBe(true);

      // Continue session
      act(() => {
        result.current.continueSession();
      });

      expect(result.current.showWarning).toBe(false);
      expect(mockToastSuccess).toHaveBeenCalledWith('Session extended');
    });
  });

  describe('Custom Callbacks', () => {
    it('should call custom onTimeout callback', () => {
      const onTimeout = jest.fn();
      
      renderHook(() => useSessionTimeout({ enabled: true, onTimeout }));

      act(() => {
        jest.advanceTimersByTime(15 * 60 * 1000);
      });

      expect(onTimeout).toHaveBeenCalled();
    });

    it('should call custom onWarning callback', () => {
      const onWarning = jest.fn();
      
      renderHook(() => useSessionTimeout({ enabled: true, onWarning }));

      act(() => {
        jest.advanceTimersByTime(14 * 60 * 1000 + 30 * 1000);
      });

      expect(onWarning).toHaveBeenCalledWith(30);
    });
  });

  describe('Event Listeners', () => {
    it('should add activity event listeners when enabled', () => {
      const addEventListenerSpy = jest.spyOn(window, 'addEventListener');
      
      renderHook(() => useSessionTimeout({ enabled: true }));

      const events = [
        'mousedown',
        'mousemove',
        'keypress',
        'scroll',
        'touchstart',
        'click',
        'keydown',
        'wheel',
      ];

      events.forEach(event => {
        expect(addEventListenerSpy).toHaveBeenCalledWith(
          event,
          expect.any(Function),
          { passive: true }
        );
      });

      addEventListenerSpy.mockRestore();
    });

    it('should remove event listeners on cleanup', () => {
      const removeEventListenerSpy = jest.spyOn(window, 'removeEventListener');
      
      const { unmount } = renderHook(() => useSessionTimeout({ enabled: true }));
      
      unmount();

      // Should have called removeEventListener for each event
      expect(removeEventListenerSpy).toHaveBeenCalled();

      removeEventListenerSpy.mockRestore();
    });
  });

  describe('resetTimer', () => {
    it('should be exposed and callable', () => {
      const { result } = renderHook(() => useSessionTimeout({ enabled: true }));

      expect(result.current.resetTimer).toBeDefined();
      expect(typeof result.current.resetTimer).toBe('function');
    });

    it('should reset the timer when called manually', () => {
      const { result } = renderHook(() => useSessionTimeout({ enabled: true }));

      // Advance 10 minutes
      act(() => {
        jest.advanceTimersByTime(10 * 60 * 1000);
      });

      // Reset manually
      act(() => {
        result.current.resetTimer();
      });

      // Advance another 10 minutes
      act(() => {
        jest.advanceTimersByTime(10 * 60 * 1000);
      });

      // Should not have logged out
      expect(mockLogout).not.toHaveBeenCalled();

      // Now complete the timeout
      act(() => {
        jest.advanceTimersByTime(5 * 60 * 1000);
      });

      expect(mockLogout).toHaveBeenCalled();
    });
  });

  describe('lastActivity', () => {
    it('should track last activity timestamp', () => {
      const before = Date.now();
      const { result } = renderHook(() => useSessionTimeout({ enabled: true }));
      const after = Date.now();

      expect(result.current.lastActivity).toBeGreaterThanOrEqual(before);
      expect(result.current.lastActivity).toBeLessThanOrEqual(after);
    });
  });
});

describe('useSessionTimeout - Edge Cases', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('should handle rapid activity events with throttling', () => {
    renderHook(() => useSessionTimeout({ enabled: true }));

    // Rapid fire events (more than once per second)
    for (let i = 0; i < 10; i++) {
      act(() => {
        window.dispatchEvent(new Event('mousemove'));
      });
    }

    // Should not cause performance issues due to throttling
    expect(mockLogout).not.toHaveBeenCalled();
  });

  it('should handle window beforeunload event', () => {
    const { unmount } = renderHook(() => useSessionTimeout({ enabled: true }));

    // Simulate beforeunload
    act(() => {
      window.dispatchEvent(new Event('beforeunload'));
    });

    unmount();

    // Should clean up without errors
    expect(mockLogout).not.toHaveBeenCalled();
  });

  it('should not start multiple timers on re-render', () => {
    const { rerender } = renderHook(() => useSessionTimeout({ enabled: true }));

    // Re-render multiple times
    rerender();
    rerender();
    rerender();

    // Advance to timeout
    act(() => {
      jest.advanceTimersByTime(15 * 60 * 1000);
    });

    // Should only logout once
    expect(mockLogout).toHaveBeenCalledTimes(1);
  });

  it('should handle disabling during active timeout', () => {
    const { rerender } = renderHook(
      ({ enabled }) => useSessionTimeout({ enabled }),
      { initialProps: { enabled: true } }
    );

    // Advance partway
    act(() => {
      jest.advanceTimersByTime(10 * 60 * 1000);
    });

    // Disable
    rerender({ enabled: false });

    // Advance past original timeout
    act(() => {
      jest.advanceTimersByTime(10 * 60 * 1000);
    });

    // Should not logout after being disabled
    expect(mockLogout).not.toHaveBeenCalled();
  });
});
