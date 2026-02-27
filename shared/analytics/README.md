# Analytics Event Architecture & Standardization

This document outlines the standardized approach to analytics event tracking across all Soul Sense platforms (Web, iOS, Android, Desktop).

## 🎯 Objective

Eliminate inconsistent event naming and schema drift by implementing strict global standards for analytics events.

## 📋 Standards

### Event Naming Convention
- **Format**: `snake_case` (lowercase with underscores)
- **No spaces**: Use underscores instead of spaces
- **No camelCase**: `buttonClick` → `button_click`
- **No prefixes**: Avoid undocumented prefixes like `viewScreen`
- **Pattern**: `^[a-z][a-z0-9_]*$`

### Examples
```javascript
// ✅ Correct
screen_view
button_click
signup_start
payment_success

// ❌ Incorrect
screenView
Screen_View
viewScreen
button-click
```

## 🏗️ Implementation

### 1. Centralized Constants

#### Web (TypeScript)
Location: `frontend-web/src/lib/utils/analytics.ts`
```typescript
export const ANALYTICS_EVENTS = {
  SCREEN_VIEW: 'screen_view',
  BUTTON_CLICK: 'button_click',
  // ... all events
} as const;
```

#### Android (Java)
Location: `mobile-app/android/app/src/main/java/com/soulsense/AnalyticsEvents.java`
```java
public final class AnalyticsEvents {
    public static final String SCREEN_VIEW = "screen_view";
    public static final String BUTTON_CLICK = "button_click";
    // ... all events
}
```

#### iOS (Swift)
Location: `mobile-app/ios/SoulSense/AnalyticsEvents.swift`
```swift
public final class AnalyticsEvents {
    public static let screenView = "screen_view"
    public static let buttonClick = "button_click"
    // ... all events
}
```

### 2. Event Schema

Location: `shared/analytics/event_schema.json`

The master schema defines:
- Required fields for all events
- Event-specific property validation
- Platform enumeration
- Schema versioning

### 3. Validation

#### Pre-commit Validation
Run validation before commits:
```bash
npm run validate:analytics  # Web
node scripts/validate_analytics.js  # Direct
```

#### CI/CD Validation
GitHub Actions workflow validates on PRs and pushes to main branches.

## 📊 Event Categories

### Screen View Events
- `screen_view` - Generic screen view
- `login_screen_view` - Login screen
- `signup_screen_view` - Signup screen
- `profile_screen_view` - Profile screen
- `settings_screen_view` - Settings screen

### User Interaction Events
- `button_click` - Generic button click
- `start_button_click` - Start button
- `login_button_click` - Login button
- `signup_button_click` - Signup button
- `logout_button_click` - Logout button

### Authentication Events
- `signup_start` - Signup process started
- `signup_success` - Signup completed
- `signup_failure` - Signup failed
- `login_attempt` - Login attempt
- `login_success` - Login success
- `login_failure` - Login failure

### Payment Events
- `payment_start` - Payment process started
- `payment_success` - Payment completed
- `payment_failure` - Payment failed

### Feature Usage Events
- `journal_entry_created` - Journal entry created
- `assessment_started` - Assessment started
- `assessment_completed` - Assessment completed
- `report_viewed` - Report viewed

### System Events
- `app_launch` - App launched
- `app_background` - App backgrounded
- `app_foreground` - App foregrounded
- `app_crash` - App crashed
- `device_rotation` - Device rotated

### Error Events
- `network_error` - Network error
- `api_error` - API error
- `validation_error` - Validation error

## 🔧 Usage Examples

### Web (TypeScript)
```typescript
import { analytics } from '@/lib/utils/analytics';

// Track screen view
analytics.trackPageView('/dashboard');

// Track button click
analytics.trackButtonClick('start-assessment', 'button');

// Track signup start
analytics.trackSignupStart('google', 'campaign_123');
```

### Android (Java)
```java
import com.soulsense.AnalyticsEvents;

// Track screen view
analytics.trackEvent(AnalyticsEvents.SCREEN_VIEW, properties);

// Track button click
analytics.trackEvent(AnalyticsEvents.BUTTON_CLICK, buttonProperties);
```

### iOS (Swift)
```swift
import SoulSense

// Track screen view
Analytics.track(event: AnalyticsEvents.screenView, properties: properties)

// Track button click
Analytics.track(event: AnalyticsEvents.buttonClick, properties: buttonProperties)
```

## ✅ Testing

### Test Cases

| ID | Scenario | Expected Result |
|----|----------|-----------------|
| AN-001 | Open Home Screen | `screen_view` event |
| AN-002 | Click Start Button | `start_button_click` event |
| AN-003 | Rotate Device | No new event variant |
| AN-004 | Invalid event name | Validation failure |

### Validation Commands
```bash
# Validate all platforms
node scripts/validate_analytics.js

# Web-specific validation
npm run validate:analytics

# Run tests
pytest tests/test_analytics_standardization.py
```

## 🚨 Risk Mitigation

### Data Fragmentation Prevention
- **Pre-commit hooks**: Block commits with invalid event names
- **CI validation**: Fail builds with schema violations
- **Cross-platform sync**: Single source of truth for event constants

### Schema Drift Prevention
- **Version control**: Schema changes require version bumps
- **Validation**: Payload validation before sending
- **Documentation**: Clear guidelines for adding new events

## 📈 Monitoring

### Dashboards
- Monitor event consistency across platforms
- Alert on unknown event names
- Track schema compliance rates

### Metrics
- Event naming consistency score
- Schema validation pass rate
- Cross-platform event parity

## 🔄 Adding New Events

1. **Add to schema**: Update `shared/analytics/event_schema.json`
2. **Update constants**: Add to all platform constant files
3. **Validate**: Run validation scripts
4. **Test**: Add test cases
5. **Document**: Update this README

## 📞 Support

For questions about analytics standardization:
- Check this document first
- Run validation scripts for issues
- Create PR with schema changes for new events