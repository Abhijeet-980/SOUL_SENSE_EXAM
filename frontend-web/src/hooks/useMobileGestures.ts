'use client';

import { useCallback, useEffect, useRef, useState } from 'react';

interface TouchPoint {
  x: number;
  y: number;
  timestamp: number;
}

interface SwipeConfig {
  onSwipeLeft?: () => void;
  onSwipeRight?: () => void;
  onSwipeUp?: () => void;
  onSwipeDown?: () => void;
  threshold?: number;
  timeout?: number;
}

interface PinchConfig {
  onPinchIn?: (scale: number) => void;
  onPinchOut?: (scale: number) => void;
  onPinchEnd?: (scale: number) => void;
}

interface LongPressConfig {
  onLongPress: () => void;
  delay?: number;
  onCancel?: () => void;
}

interface PullToRefreshConfig {
  onRefresh: () => Promise<void>;
  threshold?: number;
}

export function useSwipe(config: SwipeConfig) {
  const { 
    onSwipeLeft, 
    onSwipeRight, 
    onSwipeUp, 
    onSwipeDown, 
    threshold = 50,
    timeout = 300 
  } = config;
  
  const touchStart = useRef<TouchPoint | null>(null);
  
  const handleTouchStart = useCallback((e: TouchEvent) => {
    const touch = e.touches[0];
    touchStart.current = {
      x: touch.clientX,
      y: touch.clientY,
      timestamp: Date.now(),
    };
  }, []);
  
  const handleTouchEnd = useCallback((e: TouchEvent) => {
    if (!touchStart.current) return;
    
    const touch = e.changedTouches[0];
    const deltaX = touch.clientX - touchStart.current.x;
    const deltaY = touch.clientY - touchStart.current.y;
    const deltaTime = Date.now() - touchStart.current.timestamp;
    
    if (deltaTime > timeout) {
      touchStart.current = null;
      return;
    }
    
    const absX = Math.abs(deltaX);
    const absY = Math.abs(deltaY);
    
    if (absX > absY && absX > threshold) {
      if (deltaX > 0 && onSwipeRight) {
        onSwipeRight();
      } else if (deltaX < 0 && onSwipeLeft) {
        onSwipeLeft();
      }
    } else if (absY > absX && absY > threshold) {
      if (deltaY > 0 && onSwipeDown) {
        onSwipeDown();
      } else if (deltaY < 0 && onSwipeUp) {
        onSwipeUp();
      }
    }
    
    touchStart.current = null;
  }, [onSwipeLeft, onSwipeRight, onSwipeUp, onSwipeDown, threshold, timeout]);
  
  const ref = useRef<HTMLElement>(null);
  
  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    
    element.addEventListener('touchstart', handleTouchStart, { passive: true });
    element.addEventListener('touchend', handleTouchEnd, { passive: true });
    
    return () => {
      element.removeEventListener('touchstart', handleTouchStart);
      element.removeEventListener('touchend', handleTouchEnd);
    };
  }, [handleTouchStart, handleTouchEnd]);
  
  return ref;
}

export function usePinch(config: PinchConfig) {
  const { onPinchIn, onPinchOut, onPinchEnd } = config;
  const initialDistance = useRef<number | null>(null);
  const lastScale = useRef(1);
  
  const getDistance = (touches: TouchList): number => {
    return Math.hypot(
      touches[0].clientX - touches[1].clientX,
      touches[0].clientY - touches[1].clientY
    );
  };
  
  const handleTouchStart = useCallback((e: TouchEvent) => {
    if (e.touches.length === 2) {
      initialDistance.current = getDistance(e.touches);
    }
  }, []);
  
  const handleTouchMove = useCallback((e: TouchEvent) => {
    if (e.touches.length !== 2 || initialDistance.current === null) return;
    
    const currentDistance = getDistance(e.touches);
    const scale = currentDistance / initialDistance.current;
    
    if (scale > lastScale.current && onPinchOut) {
      onPinchOut(scale);
    } else if (scale < lastScale.current && onPinchIn) {
      onPinchIn(scale);
    }
    
    lastScale.current = scale;
  }, [onPinchIn, onPinchOut]);
  
  const handleTouchEnd = useCallback(() => {
    if (onPinchEnd && initialDistance.current !== null) {
      onPinchEnd(lastScale.current);
    }
    initialDistance.current = null;
    lastScale.current = 1;
  }, [onPinchEnd]);
  
  const ref = useRef<HTMLElement>(null);
  
  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    
    element.addEventListener('touchstart', handleTouchStart, { passive: true });
    element.addEventListener('touchmove', handleTouchMove, { passive: true });
    element.addEventListener('touchend', handleTouchEnd, { passive: true });
    
    return () => {
      element.removeEventListener('touchstart', handleTouchStart);
      element.removeEventListener('touchmove', handleTouchMove);
      element.removeEventListener('touchend', handleTouchEnd);
    };
  }, [handleTouchStart, handleTouchMove, handleTouchEnd]);
  
  return ref;
}

export function useLongPress(config: LongPressConfig) {
  const { onLongPress, delay = 500, onCancel } = config;
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>();
  const isLongPress = useRef(false);
  
  const handleTouchStart = useCallback(() => {
    isLongPress.current = false;
    timeoutRef.current = setTimeout(() => {
      isLongPress.current = true;
      onLongPress();
    }, delay);
  }, [onLongPress, delay]);
  
  const handleTouchEnd = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
    if (!isLongPress.current && onCancel) {
      onCancel();
    }
  }, [onCancel]);
  
  const handleTouchMove = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
  }, []);
  
  const ref = useRef<HTMLElement>(null);
  
  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    
    element.addEventListener('touchstart', handleTouchStart, { passive: true });
    element.addEventListener('touchend', handleTouchEnd, { passive: true });
    element.addEventListener('touchmove', handleTouchMove, { passive: true });
    
    return () => {
      element.removeEventListener('touchstart', handleTouchStart);
      element.removeEventListener('touchend', handleTouchEnd);
      element.removeEventListener('touchmove', handleTouchMove);
    };
  }, [handleTouchStart, handleTouchEnd, handleTouchMove]);
  
  return ref;
}

export function usePullToRefresh(config: PullToRefreshConfig) {
  const { onRefresh, threshold = 80 } = config;
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [pullDistance, setPullDistance] = useState(0);
  const startY = useRef(0);
  const isPulling = useRef(false);
  
  const handleTouchStart = useCallback((e: TouchEvent) => {
    if (window.scrollY === 0) {
      startY.current = e.touches[0].clientY;
      isPulling.current = true;
    }
  }, []);
  
  const handleTouchMove = useCallback((e: TouchEvent) => {
    if (!isPulling.current || isRefreshing) return;
    
    const currentY = e.touches[0].clientY;
    const distance = Math.max(0, currentY - startY.current);
    
    if (distance > 0 && window.scrollY === 0) {
      setPullDistance(Math.min(distance, threshold * 1.5));
    }
  }, [isRefreshing, threshold]);
  
  const handleTouchEnd = useCallback(async () => {
    if (!isPulling.current) return;
    
    isPulling.current = false;
    
    if (pullDistance >= threshold && !isRefreshing) {
      setIsRefreshing(true);
      try {
        await onRefresh();
      } finally {
        setIsRefreshing(false);
      }
    }
    
    setPullDistance(0);
  }, [pullDistance, threshold, isRefreshing, onRefresh]);
  
  const ref = useRef<HTMLElement>(null);
  
  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    
    element.addEventListener('touchstart', handleTouchStart, { passive: true });
    element.addEventListener('touchmove', handleTouchMove, { passive: true });
    element.addEventListener('touchend', handleTouchEnd, { passive: true });
    
    return () => {
      element.removeEventListener('touchstart', handleTouchStart);
      element.removeEventListener('touchmove', handleTouchMove);
      element.removeEventListener('touchend', handleTouchEnd);
    };
  }, [handleTouchStart, handleTouchMove, handleTouchEnd]);
  
  return { ref, isRefreshing, pullDistance };
}

export function useHapticFeedback() {
  const isSupported = typeof navigator !== 'undefined' && 'vibrate' in navigator;
  
  const light = useCallback(() => {
    if (isSupported) {
      navigator.vibrate(10);
    }
  }, [isSupported]);
  
  const medium = useCallback(() => {
    if (isSupported) {
      navigator.vibrate(20);
    }
  }, [isSupported]);
  
  const heavy = useCallback(() => {
    if (isSupported) {
      navigator.vibrate(30);
    }
  }, [isSupported]);
  
  const success = useCallback(() => {
    if (isSupported) {
      navigator.vibrate([10, 30, 10]);
    }
  }, [isSupported]);
  
  const error = useCallback(() => {
    if (isSupported) {
      navigator.vibrate([30, 50, 30]);
    }
  }, [isSupported]);
  
  return { isSupported, light, medium, heavy, success, error };
}

export function useViewportSize() {
  const [size, setSize] = useState({ width: 0, height: 0 });
  
  useEffect(() => {
    const handleResize = () => {
      setSize({
        width: window.innerWidth,
        height: window.innerHeight,
      });
    };
    
    handleResize();
    window.addEventListener('resize', handleResize);
    
    return () => window.removeEventListener('resize', handleResize);
  }, []);
  
  return size;
}

export function useBreakpoint() {
  const { width } = useViewportSize();
  
  return {
    isMobile: width < 768,
    isTablet: width >= 768 && width < 1024,
    isDesktop: width >= 1024,
    isLargeDesktop: width >= 1280,
    currentBreakpoint: width < 640 ? 'xs' : width < 768 ? 'sm' : width < 1024 ? 'md' : width < 1280 ? 'lg' : 'xl',
  };
}

export function useTouchDevice() {
  const [isTouchDevice, setIsTouchDevice] = useState(false);
  
  useEffect(() => {
    setIsTouchDevice(
      'ontouchstart' in window ||
      navigator.maxTouchPoints > 0
    );
  }, []);
  
  return isTouchDevice;
}

export function usePreventPullToRefresh() {
  useEffect(() => {
    const preventDefault = (e: TouchEvent) => {
      if (window.scrollY === 0 && e.touches.length === 1) {
        const touch = e.touches[0];
        if (touch.clientY > 0) {
          return;
        }
        e.preventDefault();
      }
    };
    
    document.body.addEventListener('touchstart', preventDefault, { passive: false });
    
    return () => {
      document.body.removeEventListener('touchstart', preventDefault);
    };
  }, []);
}
