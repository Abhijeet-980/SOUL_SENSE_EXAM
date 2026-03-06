'use client';

import React, { useState, useCallback, useRef, useEffect } from 'react';
import { Mic, MicOff, Square } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useHapticFeedback } from '@/hooks/useMobileGestures';

interface VoiceInputProps {
  onTranscript: (text: string) => void;
  onInterimTranscript?: (text: string) => void;
  language?: string;
  continuous?: boolean;
  className?: string;
  disabled?: boolean;
}

interface SpeechRecognitionEvent {
  results: SpeechRecognitionResultList;
  resultIndex: number;
}

interface SpeechRecognitionResultList {
  length: number;
  item(index: number): SpeechRecognitionResult;
  [index: number]: SpeechRecognitionResult;
}

interface SpeechRecognitionResult {
  isFinal: boolean;
  length: number;
  item(index: number): SpeechRecognitionAlternative;
  [index: number]: SpeechRecognitionAlternative;
}

interface SpeechRecognitionAlternative {
  transcript: string;
  confidence: number;
}

interface SpeechRecognition extends EventTarget {
  continuous: boolean;
  interimResults: boolean;
  lang: string;
  onresult: ((event: SpeechRecognitionEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
  onend: (() => void) | null;
  onstart: (() => void) | null;
  start(): void;
  stop(): void;
  abort(): void;
}

declare global {
  interface Window {
    SpeechRecognition: new () => SpeechRecognition;
    webkitSpeechRecognition: new () => SpeechRecognition;
  }
}

export function VoiceInput({
  onTranscript,
  onInterimTranscript,
  language = 'en-US',
  continuous = true,
  className,
  disabled = false,
}: VoiceInputProps) {
  const [isListening, setIsListening] = useState(false);
  const [isSupported, setIsSupported] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const recognitionRef = useRef<SpeechRecognition | null>(null);
  const { medium, error: errorHaptic } = useHapticFeedback();
  
  useEffect(() => {
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    setIsSupported(!!SpeechRecognition);
    
    if (SpeechRecognition) {
      recognitionRef.current = new SpeechRecognition();
      recognitionRef.current.continuous = continuous;
      recognitionRef.current.interimResults = true;
      recognitionRef.current.lang = language;
    }
    
    return () => {
      if (recognitionRef.current) {
        recognitionRef.current.abort();
      }
    };
  }, [continuous, language]);
  
  const handleResult = useCallback((event: SpeechRecognitionEvent) => {
    let interimTranscript = '';
    let finalTranscript = '';
    
    for (let i = event.resultIndex; i < event.results.length; i++) {
      const transcript = event.results[i][0].transcript;
      
      if (event.results[i].isFinal) {
        finalTranscript += transcript;
      } else {
        interimTranscript += transcript;
      }
    }
    
    if (finalTranscript) {
      onTranscript(finalTranscript);
    }
    
    if (interimTranscript && onInterimTranscript) {
      onInterimTranscript(interimTranscript);
    }
  }, [onTranscript, onInterimTranscript]);
  
  const startListening = useCallback(() => {
    if (!recognitionRef.current || isListening) return;
    
    setError(null);
    
    recognitionRef.current.onresult = handleResult;
    
    recognitionRef.current.onerror = (event: Event) => {
      const errorEvent = event as ErrorEvent;
      setError(errorEvent.message || 'Speech recognition error');
      errorHaptic();
      setIsListening(false);
    };
    
    recognitionRef.current.onend = () => {
      setIsListening(false);
    };
    
    recognitionRef.current.onstart = () => {
      setIsListening(true);
      medium();
    };
    
    try {
      recognitionRef.current.start();
    } catch (err) {
      setError('Failed to start speech recognition');
      setIsListening(false);
    }
  }, [handleResult, isListening, medium, errorHaptic]);
  
  const stopListening = useCallback(() => {
    if (!recognitionRef.current) return;
    
    recognitionRef.current.stop();
    setIsListening(false);
    medium();
  }, [medium]);
  
  const toggleListening = useCallback(() => {
    if (isListening) {
      stopListening();
    } else {
      startListening();
    }
  }, [isListening, startListening, stopListening]);
  
  if (!isSupported) {
    return null;
  }
  
  return (
    <div className={cn('relative', className)}>
      <button
        type="button"
        onClick={toggleListening}
        disabled={disabled}
        className={cn(
          'min-h-touch min-w-touch rounded-full flex items-center justify-center transition-all touch-manipulation',
          isListening
            ? 'bg-red-500 text-white animate-pulse'
            : 'bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 hover:bg-slate-200 dark:hover:bg-slate-700',
          disabled && 'opacity-50 cursor-not-allowed'
        )}
        aria-label={isListening ? 'Stop voice input' : 'Start voice input'}
        aria-pressed={isListening}
      >
        {isListening ? (
          <Square className="h-5 w-5" />
        ) : (
          <Mic className="h-5 w-5" />
        )}
      </button>
      
      {error && (
        <div className="absolute top-full mt-2 left-1/2 -translate-x-1/2 text-xs text-destructive whitespace-nowrap">
          {error}
        </div>
      )}
      
      {isListening && (
        <div className="absolute top-full mt-2 left-1/2 -translate-x-1/2 text-xs text-muted-foreground whitespace-nowrap animate-pulse">
          Listening...
        </div>
      )}
    </div>
  );
}

interface VoiceInputFieldProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  disabled?: boolean;
  rows?: number;
}

export function VoiceInputField({
  value,
  onChange,
  placeholder = 'Start typing or use voice input...',
  className,
  disabled = false,
  rows = 4,
}: VoiceInputFieldProps) {
  const [interimTranscript, setInterimTranscript] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  
  const handleTranscript = useCallback((text: string) => {
    onChange(value + (value && !value.endsWith(' ') ? ' ' : '') + text);
    setInterimTranscript('');
  }, [value, onChange]);
  
  const handleInterimTranscript = useCallback((text: string) => {
    setInterimTranscript(text);
  }, []);
  
  return (
    <div className={cn('relative', className)}>
      <textarea
        ref={textareaRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        disabled={disabled}
        rows={rows}
        className={cn(
          'w-full rounded-lg border border-input bg-background px-4 py-3 pr-14',
          'text-foreground placeholder:text-muted-foreground',
          'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
          'disabled:cursor-not-allowed disabled:opacity-50',
          'resize-none touch-manipulation'
        )}
      />
      
      <div className="absolute right-3 bottom-3">
        <VoiceInput
          onTranscript={handleTranscript}
          onInterimTranscript={handleInterimTranscript}
          disabled={disabled}
        />
      </div>
      
      {interimTranscript && (
        <div className="absolute left-4 bottom-14 text-sm text-muted-foreground italic pointer-events-none">
          {interimTranscript}
        </div>
      )}
    </div>
  );
}
