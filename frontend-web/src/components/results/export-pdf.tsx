'use client';

import React, { useRef, useState } from 'react';
import { Button } from '@/components/ui';
import { Download, Loader2 } from 'lucide-react';
import { useResults } from '@/hooks/useResults';
import { DetailedExamResult, CategoryScore, Recommendation } from '@/types/results';
// @ts-ignore - jsPDF types will be available after npm install
import jsPDF from 'jspdf';
// @ts-ignore - html2canvas types will be available after npm install
import html2canvas from 'html2canvas';
import { cn } from '@/lib/utils';

interface ExportPDFProps {
  /**
   * ID of the result to export (will fetch if not provided result prop)
   */
  resultId?: number;

  /**
   * Full result object to avoid refetching
   */
  result?: DetailedExamResult;

  /**
   * Optional user name for the report
   */
  userName?: string;

  /**
   * Additional CSS classes for the button
   */
  className?: string;

  /**
   * Button variant/size
   */
  variant?: 'default' | 'outline' | 'ghost';

  /**
   * Show loading text
   */
  showText?: boolean;
}

/**
 * Component for exporting exam results as a branded PDF document.
 * Supports both result object or resultId (with automatic fetching).
 */
export const ExportPDF: React.FC<ExportPDFProps> = ({
  resultId = undefined,
  result: initialResult = undefined,
  userName = 'Test User',
  className = undefined,
  variant = 'default',
  showText = true,
}: ExportPDFProps) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const { detailedResult, fetchDetailedResult } = useResults();

  /**
   * Main export handler
   */
  const handleExport = async () => {
    try {
      setLoading(true);
      setError(null);

      let resultData = initialResult;

      // Fetch result if only ID provided
      if (!resultData && resultId) {
        try {
          await fetchDetailedResult(resultId);
          resultData = detailedResult || undefined;
        } catch (err) {
          throw new Error('Failed to fetch exam results');
        }
      }

      if (!resultData) {
        throw new Error('No result data available');
      }

      // Generate PDF
      await generatePDF(resultData);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to export PDF';
      setError(message);
      console.error('PDF Export Error:', err);
    } finally {
      setLoading(false);
    }
  };

  /**
   * Generates PDF from result data
   */
  const generatePDF = async (resultData: DetailedExamResult) => {
    if (!contentRef.current) {
      throw new Error('Content reference not found');
    }

    // Create a temporary container for rendering
    const tempContainer = document.createElement('div');
    tempContainer.style.position = 'absolute';
    tempContainer.style.left = '-9999px';
    tempContainer.style.width = '1200px';
    tempContainer.innerHTML = generatePDFContent(resultData, userName);
    document.body.appendChild(tempContainer);

    try {
      // Render HTML to canvas
      const canvas = await html2canvas(tempContainer, {
        backgroundColor: '#ffffff',
        scale: 2,
        useCORS: true,
        logging: false,
        imageTimeout: 0,
      });

      // Create PDF
      const doc = new jsPDF({
        orientation: 'portrait',
        unit: 'mm',
        format: 'a4',
      });

      const pageWidth = doc.internal.pageSize.getWidth();
      const pageHeight = doc.internal.pageSize.getHeight();
      const imgWidth = pageWidth - 20; // 10mm margins
      const imgHeight = (canvas.height * imgWidth) / canvas.width;

      let currentY = 10;
      let imgData = canvas.toDataURL('image/png');

      // Add image to PDF, handling multiple pages if needed
      while (currentY < pageHeight) {
        if (currentY + imgHeight <= pageHeight - 10) {
          doc.addImage(imgData, 'PNG', 10, currentY, imgWidth, imgHeight);
          currentY += imgHeight;
        } else {
          // Create new page
          doc.addPage('a4');
          currentY = 10;
        }
      }

      // Add footer to all pages
      const pageCount = doc.internal.pages.length - 1;
      for (let i = 1; i <= pageCount; i++) {
        doc.setPage(i);
        doc.setFontSize(9);
        doc.setTextColor(107, 114, 128); // Gray
        const pageHeight = doc.internal.pageSize.getHeight();
        const footerText = `Page ${i} of ${pageCount} | Soul Sense © 2026`;
        doc.text(footerText, 10, pageHeight - 5);
      }

      // Generate filename with timestamp
      const timestamp = new Date().toISOString().split('T')[0];
      const filename = `Soul-Sense-Results-${timestamp}.pdf`;

      // Download PDF
      doc.save(filename);
    } finally {
      // Cleanup
      document.body.removeChild(tempContainer);
    }
  };

  return (
    <>
      <Button
        onClick={handleExport}
        disabled={loading || (!initialResult && !resultId)}
        variant={variant}
        className={cn('gap-2', className)}
        title={error || 'Export results as PDF'}
      >
        {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
        {showText && (loading ? 'Generating...' : 'Export PDF')}
      </Button>
      {error && (
        <p className="text-xs text-red-500 mt-1" role="alert">
          {error}
        </p>
      )}
    </>
  );
};

/**
 * Generates the HTML content for PDF rendering
 */
function generatePDFContent(result: DetailedExamResult, userName: string): string {
  const categoryBreakdownHTML = result.category_breakdown
    .map(
      (cat: CategoryScore) => `
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; padding: 8px 0; border-bottom: 1px solid #e5e7eb;">
      <div style="flex: 1;">
        <div style="font-weight: 500; color: #1f2937; margin-bottom: 4px;">${escapeHtml(cat.category_name)}</div>
        <div style="width: 100%; height: 8px; background-color: #e5e7eb; border-radius: 4px; overflow: hidden;">
          <div style="width: ${cat.percentage}%; height: 100%; background: linear-gradient(90deg, #3b82f6, #1d4ed8); border-radius: 4px;"></div>
        </div>
      </div>
      <div style="min-width: 50px; text-align: right; font-weight: 600; color: #1f2937; margin-left: 16px;">
        ${cat.percentage.toFixed(1)}%
      </div>
    </div>
  `
    )
    .join('');

  const recommendationsHTML = result.recommendations
    .slice(0, 5)
    .map(
      (rec: Recommendation, index: number) => `
    <div style="margin-bottom: 16px; padding: 12px; background-color: #f9fafb; border-left: 4px solid ${getPriorityColor(rec.priority)}; border-radius: 4px;">
      <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 8px;">
        <div style="font-weight: 600; color: #1f2937; flex: 1;">${escapeHtml(rec.category_name)}</div>
        <span style="display: inline-block; padding: 2px 8px; background-color: ${getPriorityBgColor(rec.priority)}; color: ${getPriorityTextColor(rec.priority)}; font-size: 12px; font-weight: 600; border-radius: 3px; text-transform: uppercase;">
          ${rec.priority}
        </span>
      </div>
      <div style="color: #4b5563; font-size: 13px; line-height: 1.5;">
        ${escapeHtml(rec.message)}
      </div>
    </div>
  `
    )
    .join('');

  const scoreColor = getScoreColor(result.overall_percentage);

  return `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <style>
          * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
          }
          body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
            background-color: #ffffff;
            color: #1f2937;
            line-height: 1.6;
          }
          .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 40px 40px;
            background-color: #ffffff;
          }
          .header {
            text-align: center;
            margin-bottom: 32px;
            padding-bottom: 24px;
            border-bottom: 3px solid #3b82f6;
          }
          .logo {
            font-size: 28px;
            font-weight: 700;
            color: #3b82f6;
            margin-bottom: 8px;
            letter-spacing: -0.5px;
          }
          .subtitle {
            font-size: 14px;
            color: #6b7280;
            margin-top: 4px;
          }
          .user-info {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 32px;
            padding: 16px;
            background-color: #f3f4f6;
            border-radius: 8px;
          }
          .user-name {
            font-size: 16px;
            font-weight: 600;
            color: #1f2937;
          }
          .test-date {
            font-size: 14px;
            color: #6b7280;
          }
          .score-section {
            display: flex;
            justify-content: center;
            align-items: center;
            margin: 32px 0;
            padding: 32px;
            background: linear-gradient(135deg, #dbeafe 0%, #f0f9ff 100%);
            border-radius: 12px;
          }
          .score-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
          }
          .score-gauge {
            width: 180px;
            height: 180px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            background: linear-gradient(135deg, ${scoreColor}20 0%, ${scoreColor}10 100%);
            border: 4px solid ${scoreColor};
            margin-bottom: 16px;
            position: relative;
          }
          .score-value {
            font-size: 48px;
            font-weight: 700;
            color: ${scoreColor};
            text-align: center;
          }
          .score-label {
            font-size: 14px;
            color: #6b7280;
            text-align: center;
            font-weight: 500;
          }
          .section {
            margin-bottom: 32px;
          }
          .section-title {
            font-size: 20px;
            font-weight: 700;
            color: #1f2937;
            margin-bottom: 16px;
            padding-bottom: 8px;
            border-bottom: 2px solid #e5e7eb;
          }
          .category-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            padding: 8px 0;
            border-bottom: 1px solid #e5e7eb;
          }
          .category-bar {
            width: 100%;
            height: 8px;
            background-color: #e5e7eb;
            border-radius: 4px;
            overflow: hidden;
            margin-bottom: 4px;
          }
          .category-fill {
            height: 100%;
            background: linear-gradient(90deg, #3b82f6, #1d4ed8);
            border-radius: 4px;
          }
          .category-name {
            font-weight: 500;
            color: #1f2937;
            margin-bottom: 4px;
          }
          .category-score {
            font-weight: 600;
            color: #1f2937;
            min-width: 50px;
            text-align: right;
          }
          .recommendation-item {
            margin-bottom: 16px;
            padding: 12px;
            background-color: #f9fafb;
            border-left: 4px solid #3b82f6;
            border-radius: 4px;
          }
          .recommendation-header {
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 8px;
          }
          .recommendation-category {
            font-weight: 600;
            color: #1f2937;
            flex: 1;
          }
          .priority-badge {
            display: inline-block;
            padding: 2px 8px;
            font-size: 11px;
            font-weight: 600;
            border-radius: 3px;
            text-transform: uppercase;
          }
          .priority-high {
            background-color: #fee2e2;
            color: #991b1b;
          }
          .priority-medium {
            background-color: #fef3c7;
            color: #92400e;
          }
          .priority-low {
            background-color: #dbeafe;
            color: #1e40af;
          }
          .recommendation-text {
            color: #4b5563;
            font-size: 13px;
            line-height: 1.5;
          }
          .footer {
            margin-top: 40px;
            padding-top: 24px;
            border-top: 2px solid #e5e7eb;
            font-size: 12px;
            color: #6b7280;
            text-align: center;
            line-height: 1.8;
          }
          .disclaimer {
            margin-top: 16px;
            padding: 12px;
            background-color: #fef3c7;
            border-radius: 4px;
            font-size: 11px;
            color: #78350f;
            line-height: 1.6;
          }
        </style>
      </head>
      <body>
        <div class="container">
          <!-- Header with Branding -->
          <div class="header">
            <div class="logo">🧠 Soul Sense</div>
            <div class="subtitle">Emotional Intelligence Assessment Report</div>
          </div>

          <!-- User Information -->
          <div class="user-info">
            <div>
              <div class="user-name">Student: ${escapeHtml(userName)}</div>
            </div>
            <div class="test-date">
              Generated: ${formatDate(new Date(result.timestamp))}
            </div>
          </div>

          <!-- Overall Score Section -->
          <div class="score-section">
            <div class="score-container">
              <div class="score-gauge">
                <div>
                  <div class="score-value">${result.overall_percentage.toFixed(1)}%</div>
                </div>
              </div>
              <div class="score-label">Overall Emotional Intelligence Score</div>
            </div>
          </div>

          <!-- Category Breakdown -->
          <div class="section">
            <div class="section-title">Category Breakdown</div>
            ${categoryBreakdownHTML}
          </div>

          <!-- Recommendations -->
          <div class="section">
            <div class="section-title">Top Recommendations</div>
            ${recommendationsHTML}
          </div>

          <!-- Footer with Disclaimer -->
          <div class="footer">
            <p>This report contains personalized insights based on the Soul Sense assessment.</p>
            <div class="disclaimer">
              <strong>Note:</strong> This assessment is designed to provide insights into emotional intelligence patterns. 
              Results should be interpreted as informational and not as a substitute for professional psychological evaluation.
              Please consult with qualified professionals for clinical interpretations.
            </div>
            <p style="margin-top: 16px; color: #9ca3af;">
              Soul Sense © 2026 | Confidential Report
            </p>
          </div>
        </div>
      </body>
    </html>
  `;
}

/**
 * Helper to escape HTML special characters
 */
function escapeHtml(text: string): string {
  const map: Record<string, string> = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;',
  };
  return text.replace(/[&<>"']/g, (m) => map[m]);
}

/**
 * Get color for score value
 */
function getScoreColor(score: number): string {
  if (score >= 85) return '#10b981'; // Emerald
  if (score >= 70) return '#3b82f6'; // Blue
  if (score >= 55) return '#f59e0b'; // Amber
  return '#ef4444'; // Red
}

/**
 * Get priority color for recommendations
 */
function getPriorityColor(priority: string): string {
  switch (priority) {
    case 'high':
      return '#ef4444';
    case 'medium':
      return '#f59e0b';
    default:
      return '#3b82f6';
  }
}

/**
 * Get priority background color
 */
function getPriorityBgColor(priority: string): string {
  switch (priority) {
    case 'high':
      return '#fee2e2';
    case 'medium':
      return '#fef3c7';
    default:
      return '#dbeafe';
  }
}

/**
 * Get priority text color
 */
function getPriorityTextColor(priority: string): string {
  switch (priority) {
    case 'high':
      return '#991b1b';
    case 'medium':
      return '#92400e';
    default:
      return '#1e40af';
  }
}

/**
 * Format date for display
 */
function formatDate(date: Date): string {
  return new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

export default ExportPDF;
