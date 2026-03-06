import * as React from 'react';
import { createPortal } from 'react-dom';
import { cn } from '@/lib/utils';
import { X } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export interface ModalProps {
  isOpen: boolean;
  onClose: () => void;
  title?: string;
  size?: 'sm' | 'md' | 'lg' | 'full';
  children: React.ReactNode;
  closeOnBackdropClick?: boolean;
  closeOnEscape?: boolean;
  showCloseButton?: boolean;
}

const Modal = React.forwardRef<HTMLDivElement, ModalProps>(
  ({
    isOpen,
    onClose,
    title,
    size = 'md',
    children,
    closeOnBackdropClick = true,
    closeOnEscape = true,
    showCloseButton = true,
    ...props
  }, ref) => {
    const [mounted, setMounted] = React.useState(false);
    const modalRef = React.useRef<HTMLDivElement>(null);
    const previousFocusRef = React.useRef<HTMLElement | null>(null);

    // Handle mounting for SSR
    React.useEffect(() => {
      setMounted(true);
    }, []);

    // Focus trap functionality
    const focusableElementsString = 'a[href], area[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled]), iframe, object, embed, [tabindex="0"], [contenteditable]';
    const focusableElements = React.useRef<NodeListOf<Element> | null>(null);

    const handleKeyDown = React.useCallback((event: KeyboardEvent) => {
      if (!isOpen) return;

      // Handle Escape key
      if (event.key === 'Escape' && closeOnEscape) {
        onClose();
        return;
      }

      // Handle Tab key for focus trap
      if (event.key === 'Tab') {
        if (!focusableElements.current) {
          focusableElements.current = modalRef.current?.querySelectorAll(focusableElementsString) || null;
        }

        if (focusableElements.current) {
          const firstElement = focusableElements.current[0] as HTMLElement;
          const lastElement = focusableElements.current[focusableElements.current.length - 1] as HTMLElement;

          if (event.shiftKey) {
            // Shift + Tab
            if (document.activeElement === firstElement) {
              lastElement.focus();
              event.preventDefault();
            }
          } else {
            // Tab
            if (document.activeElement === lastElement) {
              firstElement.focus();
              event.preventDefault();
            }
          }
        }
      }
    }, [isOpen, closeOnEscape, onClose]);

    // Set up focus trap and body scroll prevention
    React.useEffect(() => {
      if (isOpen) {
        // Store the currently focused element
        previousFocusRef.current = document.activeElement as HTMLElement;

        // Prevent body scroll
        document.body.style.overflow = 'hidden';

        // Add event listeners
        document.addEventListener('keydown', handleKeyDown);

        // Focus the modal
        setTimeout(() => {
          if (modalRef.current) {
            const firstFocusableElement = modalRef.current.querySelector(focusableElementsString) as HTMLElement;
            if (firstFocusableElement) {
              firstFocusableElement.focus();
            } else {
              modalRef.current.focus();
            }
          }
        }, 100);
      } else {
        // Restore body scroll
        document.body.style.overflow = '';

        // Restore previous focus
        if (previousFocusRef.current) {
          previousFocusRef.current.focus();
        }
      }

      return () => {
        document.body.style.overflow = '';
        document.removeEventListener('keydown', handleKeyDown);
      };
    }, [isOpen, handleKeyDown]);

    const handleBackdropClick = (event: React.MouseEvent) => {
      if (event.target === event.currentTarget && closeOnBackdropClick) {
        onClose();
      }
    };

    const sizes = {
      sm: 'max-w-md',
      md: 'max-w-lg',
      lg: 'max-w-2xl',
      full: 'max-w-full mx-4',
    };

    if (!mounted) return null;

    return createPortal(
      <AnimatePresence>
        {isOpen && (
          <>
            {/* Backdrop */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              className="fixed inset-0 z-50 bg-black/50 backdrop-blur-sm"
              onClick={handleBackdropClick}
              aria-hidden="true"
            />

            {/* Modal */}
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
              <motion.div
                ref={modalRef}
                role="dialog"
                aria-modal="true"
                aria-labelledby={title ? "modal-title" : undefined}
                aria-describedby="modal-content"
                tabIndex={-1}
                initial={{ opacity: 0, scale: 0.95, y: 20 }}
                animate={{ opacity: 1, scale: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.95, y: 20 }}
                transition={{ duration: 0.2, ease: 'easeOut' }}
                className={cn(
                  'relative w-full bg-background rounded-lg shadow-xl border',
                  sizes[size]
                )}
                {...props}
              >
                {/* Header */}
                {(title || showCloseButton) && (
                  <div className="flex items-center justify-between p-6 border-b">
                    {title && (
                      <h2
                        id="modal-title"
                        className="text-lg font-semibold text-foreground"
                      >
                        {title}
                      </h2>
                    )}
                    {showCloseButton && (
                      <button
                        onClick={onClose}
                        className="p-1 rounded-md hover:bg-muted transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
                        aria-label="Close modal"
                      >
                        <X className="h-5 w-5" />
                      </button>
                    )}
                  </div>
                )}

                {/* Content */}
                <div
                  id="modal-content"
                  className="p-6 max-h-[70vh] overflow-y-auto"
                >
                  {children}
                </div>
              </motion.div>
            </div>
          </>
        )}
      </AnimatePresence>,
      document.body
    );
  }
);

Modal.displayName = 'Modal';

export { Modal };