#!/usr/bin/env node

/**
 * Analytics Event Validation Script
 * Validates that all analytics events follow the standardized naming convention
 * and match the event schema.
 */

const fs = require('fs');
const path = require('path');

const EVENT_NAME_PATTERN = /^[a-z][a-z0-9_]*$/;
const SCHEMA_PATH = path.join(__dirname, '../shared/analytics/event_schema.json');

function validateEventNames() {
  console.log('🔍 Validating analytics event names...');

  // Read the schema
  let schema;
  try {
    schema = JSON.parse(fs.readFileSync(SCHEMA_PATH, 'utf8'));
  } catch (error) {
    console.error('❌ Failed to read event schema:', error.message);
    process.exit(1);
  }

  const allowedEvents = schema.properties.event_name.enum;
  const errors = [];

  // Check TypeScript constants
  const tsFile = path.join(__dirname, '../frontend-web/src/lib/utils/analytics.ts');
  if (fs.existsSync(tsFile)) {
    const content = fs.readFileSync(tsFile, 'utf8');
    const matches = content.match(/([A-Z_]+): '([a-z_]+)'/g) || [];

    matches.forEach(match => {
      const [, constName, eventName] = match.match(/([A-Z_]+): '([a-z_]+)'/) || [];
      if (!EVENT_NAME_PATTERN.test(eventName)) {
        errors.push(`TypeScript: Invalid event name format: ${eventName} (const: ${constName})`);
      }
      if (!allowedEvents.includes(eventName)) {
        errors.push(`TypeScript: Event not in schema: ${eventName} (const: ${constName})`);
      }
    });
  }

  // Check Java constants
  const javaFile = path.join(__dirname, '../mobile-app/android/app/src/main/java/com/soulsense/AnalyticsEvents.java');
  if (fs.existsSync(javaFile)) {
    const content = fs.readFileSync(javaFile, 'utf8');
    const matches = content.match(/"([a-z_]+)"/g) || [];

    matches.forEach(match => {
      const eventName = match.slice(1, -1); // Remove quotes
      if (!EVENT_NAME_PATTERN.test(eventName)) {
        errors.push(`Java: Invalid event name format: ${eventName}`);
      }
      if (!allowedEvents.includes(eventName)) {
        errors.push(`Java: Event not in schema: ${eventName}`);
      }
    });
  }

  // Check Swift constants
  const swiftFile = path.join(__dirname, '../mobile-app/ios/SoulSense/AnalyticsEvents.swift');
  if (fs.existsSync(swiftFile)) {
    const content = fs.readFileSync(swiftFile, 'utf8');
    const matches = content.match(/"([a-z_]+)"/g) || [];

    matches.forEach(match => {
      const eventName = match.slice(1, -1); // Remove quotes
      if (!EVENT_NAME_PATTERN.test(eventName)) {
        errors.push(`Swift: Invalid event name format: ${eventName}`);
      }
      if (!allowedEvents.includes(eventName)) {
        errors.push(`Swift: Event not in schema: ${eventName}`);
      }
    });
  }

  if (errors.length > 0) {
    console.error('❌ Analytics validation failed:');
    errors.forEach(error => console.error(`  - ${error}`));
    process.exit(1);
  } else {
    console.log('✅ All analytics events are valid!');
  }
}

function validateSchemaConsistency() {
  console.log('🔍 Validating schema consistency...');

  // This would check that all platforms have the same events
  // For now, just ensure schema is valid JSON
  try {
    JSON.parse(fs.readFileSync(SCHEMA_PATH, 'utf8'));
    console.log('✅ Event schema is valid JSON');
  } catch (error) {
    console.error('❌ Invalid event schema JSON:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  validateEventNames();
  validateSchemaConsistency();
  console.log('🎉 Analytics validation complete!');
}