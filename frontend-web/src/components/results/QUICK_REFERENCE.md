# ExportPDF Component - Quick Reference

## 📦 File Location
```
frontend-web/src/components/results/export-pdf.tsx
```

## 🚀 Quick Start

### Installation
```bash
npm install jspdf@^2.5.1 html2canvas@^1.4.1
```
*(Already in package.json)*

### Import
```tsx
import { ExportPDF } from '@/components/results';
```

### Usage
```tsx
<ExportPDF 
  result={detailedResult}
  userName="User Name"
/>
```

## 📋 Props

| Prop | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `resultId` | `number` | ❌ | — | Result ID (will fetch if provided) |
| `result` | `DetailedExamResult` | ❌ | — | Full result object (preferred) |
| `userName` | `string` | ❌ | `'Test User'` | User name for PDF header |
| `className` | `string` | ❌ | `—` | Additional CSS classes |
| `variant` | `'default' \| 'outline' \| 'ghost'` | ❌ | `'default'` | Button style variant |
| `showText` | `boolean` | ❌ | `true` | Show button text |

## ✨ Features

- ✅ Professional Soul Sense branding
- ✅ Multi-section report (score, categories, recommendations)
- ✅ Color-coded visualizations
- ✅ Automatic page breaks
- ✅ Page numbers and footers
- ✅ Loading states
- ✅ Error handling
- ✅ Browser compatible

## 📊 PDF Sections

1. **Header** - Soul Sense branding
2. **User Info** - Name, date, timestamp
3. **Overall Score** - Circular gauge, percentage
4. **Categories** - Performance bars, percentages
5. **Recommendations** - Top 5 with priorities
6. **Footer** - Disclaimer, copyright, page numbers

## 🎨 Styling

All styles are **inline** in the generated HTML for reliability. Colors:

| Element | Color | Hex |
|---------|-------|-----|
| Primary | Blue | `#3B82F6` |
| Success | Green | `#10B981` |
| Warning | Amber | `#F59E0B` |
| Danger | Red | `#EF4444` |

## 🔄 Data Flow

```
Props → Validation → Fetch (if needed) → HTML Generation → Canvas → PDF → Download
```

## 📱 Browser Support

| Browser | Support |
|---------|---------|
| Chrome | ✅ Full |
| Firefox | ✅ Full |
| Safari | ✅ Full |
| Edge | ✅ Full |
| Mobile | ✅ Full |

## ⚠️ Common Issues

| Issue | Solution |
|-------|----------|
| Module not found | Run `npm install` |
| PDF blank | Check result data structure |
| Styling wrong | Verify inline styles generated |
| Download fails | Check browser permissions |

## 🧪 Testing Checklist

- [ ] Export with result object
- [ ] Export with result ID (fetch)
- [ ] Custom user name displays
- [ ] All PDF sections present
- [ ] Colors display correctly
- [ ] Text is readable
- [ ] Multi-page works
- [ ] Works on mobile

## 📚 Related Files

| File | Purpose |
|------|---------|
| `export-pdf.tsx` | Main component |
| `EXPORT_PDF_GUIDE.md` | Full documentation |
| `EXPORT_PDF_INTEGRATION_EXAMPLE.tsx` | Usage example |
| `score-gauge.tsx` | Score visualization |
| `category-breakdown.tsx` | Category display |

## 🔧 Key Functions

### `handleExport()`
Coordinates the export process:
1. Validates data
2. Fetches if needed
3. Generates PDF
4. Triggers download

### `generatePDF()`
Creates PDF from result object:
1. Renders HTML content
2. Converts to canvas
3. Creates PDF document
4. Adds footers
5. Downloads file

## 💾 File Naming

Generated PDFs follow this pattern:
```
Soul-Sense-Results-YYYY-MM-DD.pdf
```

Example: `Soul-Sense-Results-2026-02-18.pdf`

## 🚨 Required Properties

For the component to work, `result` object must have:

```typescript
{
  assessment_id: number,
  total_score: number,
  overall_percentage: number,
  timestamp: string,
  category_breakdown: [
    {
      category_name: string,
      score: number,
      percentage: number
    }
  ],
  recommendations: [
    {
      category_name: string,
      message: string,
      priority: 'high' | 'medium' | 'low'
    }
  ]
}
```

## 🎯 Integration Steps

1. **Import component**
   ```tsx
   import { ExportPDF } from '@/components/results';
   ```

2. **Place in JSX**
   ```tsx
   <ExportPDF result={detailedResult} userName={userName} />
   ```

3. **Install dependencies** (if not done)
   ```bash
   npm install
   ```

4. **Test the feature**
   - Click Export PDF button
   - Verify PDF downloads
   - Check content and styling

## 📞 Support

For issues:
1. Check browser console (F12) for errors
2. Verify result data structure
3. Ensure npm packages are installed
4. Review EXPORT_PDF_GUIDE.md
5. Check component props

## 📝 Notes

- Component is client-side only
- All PDF generation happens in browser
- No data sent to server for PDF
- Works offline after initial load
- Modern browsers required (ES6+)

---

**Version**: 1.0.0  
**Last Updated**: February 2026  
**Status**: ✅ Production Ready
