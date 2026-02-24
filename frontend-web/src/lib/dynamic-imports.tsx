/**
 * Dynamic imports for heavy components to enable code splitting and lazy loading.
 * This reduces initial bundle size and improves Time to Interactive (TTI).
 */

import dynamic from 'next/dynamic';
import { ComponentType } from 'react';
import React from 'react';

// Chart components from recharts - lazy loaded to reduce initial bundle
export const ActivityAreaChart = dynamic(
  () => import('@/components/dashboard/charts/activity-area-chart').then(mod => ({ default: mod.ActivityAreaChart })),
  {
    loading: () => (
      <div className="h-[250px] w-full animate-pulse bg-slate-100 dark:bg-slate-800 rounded-lg" />
    ),
    ssr: true,
  }
) as ComponentType<any>;

export const ContributionMix = dynamic(
  () => import('@/components/dashboard/charts/contribution-mix').then(mod => ({ default: mod.ContributionMix })),
  {
    loading: () => (
      <div className="h-[200px] w-full animate-pulse bg-slate-100 dark:bg-slate-800 rounded-lg" />
    ),
    ssr: true,
  }
) as ComponentType<any>;

export const ForceDirectedGraph = dynamic(
  () => import('@/components/dashboard/charts/force-directed-graph').then(mod => ({ default: mod.ForceDirectedGraph })),
  {
    loading: () => (
      <div className="h-[300px] w-full animate-pulse bg-slate-100 dark:bg-slate-800 rounded-lg" />
    ),
    ssr: false, // Force graph requires browser APIs
  }
) as ComponentType<any>;

export const RepositorySunburst = dynamic(
  () => import('@/components/dashboard/charts/repository-sunburst').then(mod => ({ default: mod.RepositorySunburst })),
  {
    loading: () => (
      <div className="h-[300px] w-full animate-pulse bg-slate-100 dark:bg-slate-800 rounded-lg" />
    ),
    ssr: true,
  }
) as ComponentType<any>;

// Results chart components
export const HistoryChart = dynamic(
  () => import('@/components/results/history-chart').then(mod => ({ default: mod.HistoryChart })),
  {
    loading: () => (
      <div className="h-[250px] w-full animate-pulse bg-slate-100 dark:bg-slate-800 rounded-lg" />
    ),
    ssr: true,
  }
) as ComponentType<any>;

export const ScoreGauge = dynamic(
  () => import('@/components/results/score-gauge').then(mod => ({ default: mod.ScoreGauge })),
  {
    loading: () => (
      <div className="h-[180px] w-full animate-pulse bg-slate-100 dark:bg-slate-800 rounded-lg" />
    ),
    ssr: true,
  }
) as ComponentType<any>;

// PDF export - heavy libraries (jspdf, html2canvas)
export const ExportPDF = dynamic(
  () => import('@/components/results/export-pdf').then(mod => ({ default: mod.ExportPDF })),
  {
    loading: () => null, // Show button in loading state
    ssr: false, // PDF generation requires browser APIs
  }
) as ComponentType<any>;

// Mood trend chart
export const MoodTrend = dynamic(
  () => import('@/components/journal/mood-trend').then(mod => ({ default: mod.MoodTrend })),
  {
    loading: () => (
      <div className="h-[200px] w-full animate-pulse bg-slate-100 dark:bg-slate-800 rounded-lg" />
    ),
    ssr: true,
  }
) as ComponentType<any>;
