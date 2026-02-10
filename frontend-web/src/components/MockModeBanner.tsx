'use client';

import React from 'react';
import { useAuth } from '@/hooks/useAuth';

/**
 * MockModeBanner Component
 * 
 * Displays a visual banner when the application is running in mock authentication mode.
 * This helps developers and testers identify when they're using mock credentials.
 */
export const MockModeBanner: React.FC = () => {
    const { isMockMode } = useAuth();

    if (!isMockMode) {
        return null;
    }

    return (
        <div className="mock-mode-banner">
            <div className="mock-mode-content">
                <span className="mock-mode-icon">🎭</span>
                <span className="mock-mode-text">
                    <strong>Mock Authentication Mode</strong> - Using test credentials
                </span>
                <a
                    href="/docs/MOCK_AUTH.md"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="mock-mode-link"
                >
                    Learn more
                </a>
            </div>

            <style jsx>{`
                .mock-mode-banner {
                    position: fixed;
                    top: 0;
                    left: 0;
                    right: 0;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 8px 16px;
                    z-index: 9999;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
                    animation: slideDown 0.3s ease-out;
                }

                @keyframes slideDown {
                    from {
                        transform: translateY(-100%);
                        opacity: 0;
                    }
                    to {
                        transform: translateY(0);
                        opacity: 1;
                    }
                }

                .mock-mode-content {
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    gap: 12px;
                    max-width: 1200px;
                    margin: 0 auto;
                }

                .mock-mode-icon {
                    font-size: 20px;
                    animation: pulse 2s ease-in-out infinite;
                }

                @keyframes pulse {
                    0%, 100% {
                        transform: scale(1);
                    }
                    50% {
                        transform: scale(1.1);
                    }
                }

                .mock-mode-text {
                    font-size: 14px;
                    line-height: 1.4;
                }

                .mock-mode-text strong {
                    font-weight: 600;
                }

                .mock-mode-link {
                    color: white;
                    text-decoration: underline;
                    font-size: 13px;
                    opacity: 0.9;
                    transition: opacity 0.2s;
                }

                .mock-mode-link:hover {
                    opacity: 1;
                }

                @media (max-width: 768px) {
                    .mock-mode-content {
                        flex-direction: column;
                        gap: 4px;
                        text-align: center;
                    }

                    .mock-mode-text {
                        font-size: 12px;
                    }
                }
            `}</style>
        </div>
    );
};

/**
 * MockModeIndicator Component
 * 
 * A smaller, corner-positioned indicator for mock mode.
 * Use this as an alternative to the banner for a less intrusive indicator.
 */
export const MockModeIndicator: React.FC = () => {
    const { isMockMode } = useAuth();
    const [isExpanded, setIsExpanded] = React.useState(false);

    if (!isMockMode) {
        return null;
    }

    return (
        <div
            className="mock-mode-indicator"
            onMouseEnter={() => setIsExpanded(true)}
            onMouseLeave={() => setIsExpanded(false)}
        >
            <div className="indicator-icon">🎭</div>
            {isExpanded && (
                <div className="indicator-tooltip">
                    Mock Auth Mode Active
                </div>
            )}

            <style jsx>{`
                .mock-mode-indicator {
                    position: fixed;
                    bottom: 20px;
                    right: 20px;
                    z-index: 9998;
                    cursor: help;
                }

                .indicator-icon {
                    width: 48px;
                    height: 48px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    border-radius: 50%;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 24px;
                    box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
                    animation: bounce 2s ease-in-out infinite;
                }

                @keyframes bounce {
                    0%, 100% {
                        transform: translateY(0);
                    }
                    50% {
                        transform: translateY(-5px);
                    }
                }

                .indicator-tooltip {
                    position: absolute;
                    bottom: 60px;
                    right: 0;
                    background: rgba(0, 0, 0, 0.9);
                    color: white;
                    padding: 8px 12px;
                    border-radius: 6px;
                    font-size: 12px;
                    white-space: nowrap;
                    animation: fadeIn 0.2s ease-out;
                }

                @keyframes fadeIn {
                    from {
                        opacity: 0;
                        transform: translateY(5px);
                    }
                    to {
                        opacity: 1;
                        transform: translateY(0);
                    }
                }

                .indicator-tooltip::after {
                    content: '';
                    position: absolute;
                    bottom: -6px;
                    right: 20px;
                    width: 0;
                    height: 0;
                    border-left: 6px solid transparent;
                    border-right: 6px solid transparent;
                    border-top: 6px solid rgba(0, 0, 0, 0.9);
                }
            `}</style>
        </div>
    );
};

export default MockModeBanner;
