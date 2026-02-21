/**
 * EXAMPLE: How to integrate ExportPDF component in Results Detail Page
 *
 * This shows how to update the existing results/[id]/page.tsx to use
 * the new ExportPDF component instead of the placeholder handleExport.
 *
 * NOTE: This is a reference implementation. Use the patterns shown here
 * to update your own results pages.
 */

'use client';

import React, { useEffect } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { useResults } from '@/hooks/useResults';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Button,
  Skeleton,
} from '@/components/ui';
import {
  ScoreGauge,
  CategoryBreakdown,
  RecommendationCard,
  ExportPDF, // NEW: Import the ExportPDF component
} from '@/components/results';
import { ArrowLeft, RefreshCw, Calendar, Clock } from 'lucide-react';

export default function ResultDetailPage() {
  const params = useParams();
  const router = useRouter();
  const rawId = params?.id as string | string[] | undefined;
  const examId = rawId ? parseInt(Array.isArray(rawId) ? rawId[0] : rawId, 10) : NaN;

  // CHANGED: Use fetchDetailedResult hook before early return
  const { detailedResult, loading, error, fetchDetailedResult } = useResults();

  useEffect(() => {
    if (examId && !detailedResult) {
      fetchDetailedResult(examId);
    }
  }, [examId, detailedResult, fetchDetailedResult]);

  if (!examId || Number.isNaN(examId)) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <Card className="max-w-md">
          <CardHeader>
            <CardTitle>Invalid result</CardTitle>
            <CardDescription>
              The requested result ID is invalid. Please check the link and try again.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={() => router.push('/results')}>
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to results
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Format date
  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  // Format duration
  const formatDuration = (seconds: number) => {
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes}m ${remainingSeconds}s`;
  };

  // Handle retake
  const handleRetake = () => {
    router.push('/exam');
  };

  // CHANGED: Removed handleExport function - now using ExportPDF component

  // Loading state
  if (loading) {
    return (
      <div className="space-y-6">
        <div className="flex items-center gap-4">
          <Skeleton className="h-10 w-10" />
          <Skeleton className="h-8 w-64" />
        </div>
        <Skeleton className="h-[400px] w-full" />
        <Skeleton className="h-[300px] w-full" />
      </div>
    );
  }

  // Error state
  if (error || !detailedResult) {
    return (
      <div className="space-y-6">
        <Button variant="ghost" onClick={() => router.back()}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Card className="border-red-200 bg-red-50 dark:border-red-900 dark:bg-red-950">
          <CardHeader>
            <CardTitle className="text-red-900 dark:text-red-100">Error Loading Results</CardTitle>
            <CardDescription className="text-red-700 dark:text-red-300">
              {error || 'Unable to load exam results. Please try again.'}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={() => router.push('/results')}>View All Results</Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Transform categories data for CategoryBreakdown component
  const categoryScores = detailedResult.category_breakdown.map((cat) => ({
    name: cat.category_name,
    score: cat.percentage,
  }));

  return (
    <div className="space-y-8 pb-12">
      {/* Header with Back Button */}
      <div className="flex items-center justify-between">
        <Button variant="ghost" onClick={() => router.back()}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Results
        </Button>

        <div className="flex gap-2 no-print">
          <Button variant="outline" onClick={handleRetake}>
            <RefreshCw className="mr-2 h-4 w-4" />
            Retake Exam
          </Button>

          {/* NEW: ExportPDF Component - replaces handleExport button */}
          <ExportPDF
            result={detailedResult}
            userName="Student Name" // Replace with actual user name from state/auth
            variant="default"
            showText={true}
          />
        </div>
      </div>

      {/* Page Title & Metadata */}
      <div className="space-y-2">
        <h1 className="text-4xl font-bold tracking-tight">Exam Results</h1>
        <div className="flex flex-wrap gap-4 text-sm text-muted-foreground">
          <div className="flex items-center gap-2">
            <Calendar className="h-4 w-4" />
            <span>{formatDate(detailedResult.timestamp)}</span>
          </div>
        </div>
      </div>

      {/* Overall Score Section */}
      <Card className="overflow-hidden">
        <CardHeader className="bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-blue-950 dark:to-indigo-950">
          <CardTitle>Overall Performance</CardTitle>
          <CardDescription>Your comprehensive emotional intelligence score</CardDescription>
        </CardHeader>
        <CardContent className="pt-8 pb-8 flex justify-center">
          <ScoreGauge
            score={detailedResult.overall_percentage}
            size="lg"
            label="Overall Score"
            animated
          />
        </CardContent>
      </Card>

      {/* Category Breakdown */}
      {detailedResult.category_breakdown && detailedResult.category_breakdown.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Category Breakdown</CardTitle>
            <CardDescription>
              Detailed performance across emotional intelligence dimensions
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-6">
            <CategoryBreakdown categories={categoryScores} showLabels animated />
          </CardContent>
        </Card>
      )}

      {/* Recommendations */}
      {detailedResult.recommendations && detailedResult.recommendations.length > 0 && (
        <div className="space-y-4">
          <div>
            <h2 className="text-2xl font-bold tracking-tight">Personalized Recommendations</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Tailored suggestions to improve your emotional intelligence
            </p>
          </div>
          <div className="grid gap-4 md:grid-cols-2">
            {detailedResult.recommendations.map((recommendation, index) => (
              <RecommendationCard
                key={`${recommendation.category_name}-${index}`}
                recommendation={recommendation}
              />
            ))}
          </div>
        </div>
      )}

      {/* Action Buttons (Mobile-friendly, bottom) */}
      <div className="flex flex-col sm:flex-row gap-3 no-print pt-4 border-t">
        <Button variant="outline" onClick={handleRetake} className="flex-1">
          <RefreshCw className="mr-2 h-4 w-4" />
          Retake Exam
        </Button>

        {/* NEW: ExportPDF Component - also shown at bottom for mobile users */}
        <ExportPDF
          result={detailedResult}
          userName="Student Name"
          variant="default"
          showText={true}
          className="flex-1"
        />
      </div>

      {/* Print Styles */}
      <style jsx global>{`
        @media print {
          .no-print {
            display: none !important;
          }

          body {
            print-color-adjust: exact;
            -webkit-print-color-adjust: exact;
          }

          @page {
            margin: 1cm;
          }
        }
      `}</style>
    </div>
  );
}
