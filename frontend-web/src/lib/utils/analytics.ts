/**
 * Analytics Utility with Standardized Event Names
 * All event names follow strict snake_case convention.
 * Event Schema Version: 1.0
 */

// Event name constants - must match shared/analytics/event_schema.json
export const ANALYTICS_EVENTS = {
  // Screen view events
  SCREEN_VIEW: 'screen_view',
  LOGIN_SCREEN_VIEW: 'login_screen_view',
  SIGNUP_SCREEN_VIEW: 'signup_screen_view',
  PROFILE_SCREEN_VIEW: 'profile_screen_view',
  SETTINGS_SCREEN_VIEW: 'settings_screen_view',

  // User interaction events
  BUTTON_CLICK: 'button_click',
  START_BUTTON_CLICK: 'start_button_click',
  LOGIN_BUTTON_CLICK: 'login_button_click',
  SIGNUP_BUTTON_CLICK: 'signup_button_click',
  LOGOUT_BUTTON_CLICK: 'logout_button_click',

  // Authentication events
  SIGNUP_START: 'signup_start',
  SIGNUP_SUCCESS: 'signup_success',
  SIGNUP_FAILURE: 'signup_failure',
  LOGIN_ATTEMPT: 'login_attempt',
  LOGIN_SUCCESS: 'login_success',
  LOGIN_FAILURE: 'login_failure',

  // Payment events
  PAYMENT_START: 'payment_start',
  PAYMENT_SUCCESS: 'payment_success',
  PAYMENT_FAILURE: 'payment_failure',

  // Feature usage events
  JOURNAL_ENTRY_CREATED: 'journal_entry_created',
  ASSESSMENT_STARTED: 'assessment_started',
  ASSESSMENT_COMPLETED: 'assessment_completed',
  REPORT_VIEWED: 'report_viewed',

  // System events
  APP_LAUNCH: 'app_launch',
  APP_BACKGROUND: 'app_background',
  APP_FOREGROUND: 'app_foreground',
  APP_CRASH: 'app_crash',
  DEVICE_ROTATION: 'device_rotation',

  // Error events
  NETWORK_ERROR: 'network_error',
  API_ERROR: 'api_error',
  VALIDATION_ERROR: 'validation_error',
} as const;

type AnalyticsEventName = typeof ANALYTICS_EVENTS[keyof typeof ANALYTICS_EVENTS];

interface AnalyticsEvent {
  event_name: AnalyticsEventName;
  timestamp: string;
  user_id?: string;
  session_id: string;
  platform: 'web' | 'ios' | 'android' | 'desktop';
  app_version: string;
  device_info?: {
    model?: string;
    os_version?: string;
    screen_resolution?: string;
  };
  event_properties?: Record<string, any>;
}

interface AnalyticsConfig {
  enabled: boolean;
  provider?: 'vercel' | 'ga4' | 'mixpanel' | 'console';
}

// Schema validation function
function validateEventSchema(event: AnalyticsEvent): boolean {
  // Basic validation - in production, use a proper JSON schema validator
  const requiredFields = ['event_name', 'timestamp', 'session_id', 'platform', 'app_version'];
  for (const field of requiredFields) {
    if (!(field in event)) {
      console.error(`[Analytics] Missing required field: ${field}`);
      return false;
    }
  }

  // Validate event name format (snake_case)
  if (!/^[a-z][a-z0-9_]*$/.test(event.event_name)) {
    console.error(`[Analytics] Invalid event name format: ${event.event_name}`);
    return false;
  }

  return true;
}

class AnalyticsManager {
  private config: AnalyticsConfig = { enabled: false };
  private sessionId: string;

  constructor() {
    this.sessionId = this.generateSessionId();
  }

  private generateSessionId(): string {
    return `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  configure(config: AnalyticsConfig) {
    this.config = { ...this.config, ...config };
  }

  trackPageView(url: string) {
    if (!this.config.enabled) return;

    const event: AnalyticsEvent = {
      event_name: ANALYTICS_EVENTS.SCREEN_VIEW,
      timestamp: new Date().toISOString(),
      session_id: this.sessionId,
      platform: 'web',
      app_version: process.env.NEXT_PUBLIC_APP_VERSION || '1.0.0',
      event_properties: { screen_name: url }
    };

    this.trackEvent(event);
  }

  trackEvent(event: AnalyticsEvent) {
    if (!this.config.enabled) return;

    if (!validateEventSchema(event)) {
      console.error('[Analytics] Event validation failed, not tracking');
      return;
    }

    // Send to configured provider
    switch (this.config.provider) {
      case 'vercel':
        // va.track(event.event_name, event);
        break;
      case 'ga4':
        // gtag('event', event.event_name, event);
        break;
      case 'mixpanel':
        // mixpanel.track(event.event_name, event);
        break;
      default:
        console.log(`[Analytics] Event: ${event.event_name}`, event);
    }
  }

  // Convenience methods for common events
  trackButtonClick(buttonName: string, elementType: 'button' | 'link' | 'menu_item' = 'button') {
    this.trackEvent({
      event_name: ANALYTICS_EVENTS.BUTTON_CLICK,
      timestamp: new Date().toISOString(),
      session_id: this.sessionId,
      platform: 'web',
      app_version: process.env.NEXT_PUBLIC_APP_VERSION || '1.0.0',
      event_properties: { button_name: buttonName, element_type: elementType }
    });
  }

  trackSignupStart(method: 'email' | 'google' | 'apple' | 'facebook', referralCode?: string) {
    this.trackEvent({
      event_name: ANALYTICS_EVENTS.SIGNUP_START,
      timestamp: new Date().toISOString(),
      session_id: this.sessionId,
      platform: 'web',
      app_version: process.env.NEXT_PUBLIC_APP_VERSION || '1.0.0',
      event_properties: { method, referral_code: referralCode }
    });
  }

  trackError(errorType: 'network' | 'api' | 'validation', errorCode?: string, errorMessage?: string) {
    const eventName = errorType === 'network' ? ANALYTICS_EVENTS.NETWORK_ERROR :
                     errorType === 'api' ? ANALYTICS_EVENTS.API_ERROR :
                     ANALYTICS_EVENTS.VALIDATION_ERROR;

    this.trackEvent({
      event_name: eventName,
      timestamp: new Date().toISOString(),
      session_id: this.sessionId,
      platform: 'web',
      app_version: process.env.NEXT_PUBLIC_APP_VERSION || '1.0.0',
      event_properties: { error_code: errorCode, error_message: errorMessage }
    });
  }
}

export const analytics = new AnalyticsManager();
