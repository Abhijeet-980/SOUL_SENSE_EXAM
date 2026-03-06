# PDF Export Component Guide

## Overview

The `ExportPDF` component provides client-side PDF generation for exam results. It creates a professional, branded PDF report with the Soul Sense branding, user information, scores, category breakdowns, and personalized recommendations.

## Features

✅ **Professional Branding** - Soul Sense header and styling  
✅ **Complete Report** - All required sections included  
✅ **Loading States** - Visual feedback during generation  
✅ **Error Handling** - Graceful error messages  
✅ **Multi-Page Support** - Handles long content automatically  
✅ **Browser Compatible** - Works on all modern browsers  
✅ **Print-Friendly** - PDF designed for printing  

## Installation

The component requires two PDF libraries to be installed:

```bash
npm install jspdf@^2.5.1 html2canvas@^1.4.1
```

These are already added to `package.json` in the dependencies.

## Usage

### Basic Props

```typescript
interface ExportPDFProps {
  // Either provide resultId (will fetch if needed)
  resultId?: number;

  // Or provide the full result object (recommended to avoid refetching)
  result?: DetailedExamResult;

  // User name to display in the PDF (optional)
  userName?: string;

  // CSS classes for styling (optional)
  className?: string;

  // Button variant (optional, default: 'default')
  variant?: 'default' | 'outline' | 'ghost';

  // Show text on button (optional, default: true)
  showText?: boolean;
}
```

### Example 1: With Result Object (Recommended)

```tsx
import { ExportPDF } from '@/components/results';
import { useResults } from '@/hooks/useResults';

function ResultsPage() {
  const { detailedResult } = useResults();

  return (
    <div className="flex gap-2">
      <ExportPDF 
        result={detailedResult}
        userName="John Doe"
      />
    </div>
  );
}
```

### Example 2: With Result ID

```tsx
import { ExportPDF } from '@/components/results';

function ResultCard() {
  return (
    <ExportPDF 
      resultId={123}
      userName="Jane Smith"
    />
  );
}
```

### Example 3: In Results Detail Page

```tsx
'use client';

import React from 'react';
import { useParams } from 'next/navigation';
import { useResults } from '@/hooks/useResults';
import { ExportPDF, ScoreGauge, CategoryBreakdown } from '@/components/results';
import { Button } from '@/components/ui/button';

export default function ResultDetailPage() {
  const params = useParams();
  const examId = parseInt(params.id as string, 10);
  const { detailedResult, loading, error } = useResults();

  if (loading) return <div>Loading...</div>;
  if (error || !detailedResult) return <div>Error loading results</div>;

  return (
    <div className="space-y-8">
      {/* Header with Export Button */}
      <div className="flex items-center justify-between">
        <h1 className="text-3xl font-bold">Exam Results</h1>
        
        <div className="flex gap-2">
          <ExportPDF 
            result={detailedResult}
            userName={detailedResult.user_name}
            variant="default"
          />
        </div>
      </div>

      {/* Score Visualization */}
      <ScoreGauge 
        score={detailedResult.overall_percentage} 
        size="lg"
      />

      {/* Category Breakdown */}
      <CategoryBreakdown 
        categories={detailedResult.category_breakdown.map(cat => ({
          name: cat.category_name,
          score: cat.percentage,
        }))}
      />
    </div>
  );
}
```

### Example 4: Styled with Custom Classes

```tsx
<ExportPDF 
  result={result}
  userName="User Name"
  className="w-full md:w-auto"
  variant="outline"
  showText={true}
/>
```

## PDF Report Sections

The generated PDF includes:

### 1. **Header** 
- Soul Sense branding/logo
- "Emotional Intelligence Assessment Report" subtitle

### 2. **User Information**
- Student name
- Assessment date and time
- Generated timestamp

### 3. **Overall Score**
- Large circular gauge visualization
- Percentage score (0-100)
- Color-coded by performance level

### 4. **Category Breakdown**
- All EQ categories (Self-Awareness, Empathy, etc.)
- Individual percentage scores
- Visual progress bars for each category
- Color indicators for performance

### 5. **Top Recommendations**
- Up to 5 highest-priority recommendations
- Category name for each recommendation
- Priority level badge (High/Medium/Low)
- Actionable advice text

### 6. **Footer**
- Page numbers (on multi-page reports)
- Legal disclaimer about assessment usage
- Copyright notice

## Color Scheme

The PDF uses the following color scheme:

- **Headers**: Blue (#3B82F6)
- **Score Gauge Colors**:
  - Excellent (85+): Green (#10B981)
  - Good (70-84): Blue (#3B82F6)
  - Fair (55-69): Amber (#F59E0B)
  - Poor (<55): Red (#EF4444)
- **Recommendation Priority**:
  - High: Red (#EF4444)
  - Medium: Amber (#F59E0B)
  - Low: Blue (#3B82F6)

## State Management

The component manages its own loading and error states:

```tsx
const [loading, setLoading] = useState(false);
const [error, setError] = useState<string | null>(null);
```

**Loading State**: Shows spinner icon and "Generating..." text  
**Error State**: Displays error message below the button  
**Success**: Automatically downloads the PDF file

## File Naming

Generated PDFs are named using the following pattern:

```
Soul-Sense-Results-YYYY-MM-DD.pdf
```

Example: `Soul-Sense-Results-2026-02-18.pdf`

## Technical Details

### PDF Generation Process

1. **HTML Content Rendering**: Component generates formatted HTML with inline styles
2. **Canvas Conversion**: Uses `html2canvas` to convert HTML to image
3. **PDF Creation**: Uses `jsPDF` to create PDF document
4. **Multi-Page Handling**: Automatically creates new pages if content exceeds one page
5. **Footer Addition**: Adds page numbers and copyright to all pages
6. **Download**: Browser downloads the PDF automatically

### Styling

All styles are inline (not using CSS classes) to ensure they render correctly in the PDF. This includes:
- Font families (system fonts)
- Colors and gradients
- Spacing and sizing
- Border styles
- Background colors

### Browser Compatibility

✅ Chrome/Chromium  
✅ Firefox  
✅ Safari  
✅ Edge  
✅ Mobile browsers (iOS Safari, Chrome Mobile)

## Error Handling

The component handles several error scenarios:

```typescript
// No data provided
if (!resultData) {
  throw new Error('No result data available');
}

// Failed to fetch result
catch (err) {
  throw new Error('Failed to fetch exam results');
}

// Content reference not found
if (!contentRef.current) {
  throw new Error('Content reference not found');
}
```

All errors are caught and displayed to the user in a friendly format.

## Performance Optimization

- **Lazy Loading**: PDF libraries only loaded when component is used
- **Image Optimization**: HTML2Canvas uses optimized settings for faster rendering
- **Memory Management**: Temporary DOM elements are cleaned up after generation
- **Async Processing**: PDF generation doesn't block UI

## Accessibility

- Button includes proper `aria-label` and `title` attributes
- Error messages have `role="alert"` for screen readers
- Semantic HTML in generated PDF

## Testing

To test the PDF export component:

1. Navigate to a results page
2. Click the "Export PDF" button
3. Wait for the loading spinner to complete
4. PDF should automatically download
5. Open the PDF to verify:
   - All sections are present
   - Formatting looks correct
   - Colors display properly
   - Text is readable

### Test Cases

- ✅ Export with full result object (no refetch)
- ✅ Export with result ID (with fetch)
- ✅ Export with custom user name
- ✅ Export with different button variants
- ✅ Export error handling
- ✅ Multi-page PDF generation
- ✅ Different screen sizes
- ✅ Different browsers

## Troubleshooting

### PDF doesn't download

Check browser console for errors and ensure:
- Dependencies are installed (`npm install jspdf html2canvas`)
- Result data is valid
- Browser allows downloads

### PDF styling looks wrong

- Verify CSS is inline (not using external stylesheets)
- Check browser console for canvas rendering errors
- Ensure fonts are system fonts

### Loading never completes

- Check network requests for API errors
- Verify result data is available
- Check browser console for JavaScript errors

## Future Enhancements

Potential improvements for future versions:

- [ ] Add logo/branding image upload
- [ ] Customize colors and fonts
- [ ] Add chart visualizations (trend graphs)
- [ ] Add reflection/notes section
- [ ] Support for multiple languages
- [ ] Server-side PDF generation
- [ ] Email PDF directly
- [ ] Signature capture
- [ ] QR code for digital verification

## Support

For issues or questions about the PDF export component:

1. Check this guide
2. Review error messages in console
3. Check browser compatibility
4. Verify dependencies are installed
5. Check component props are correct

## Related Components

- [ScoreGauge](./score-gauge.tsx) - Circular score visualization
- [CategoryBreakdown](./category-breakdown.tsx) - Category performance display
- [RecommendationCard](./recommendation-card.tsx) - Recommendation display
