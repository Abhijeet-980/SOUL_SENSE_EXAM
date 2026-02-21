import { cn } from "@/lib/utils"

/**
 * 1. Base Skeleton Component
 * - Implements the animated shimmer effect.
 * - Adapts to light (bg-slate-200) and dark (bg-slate-800) modes.
 */
function Skeleton({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        // Base colors
        "relative overflow-hidden rounded-md bg-slate-200 dark:bg-slate-800",
        // animate-shimmer-slide
        "after:absolute after:inset-0 after:-translate-x-full after:animate-shimmer-slide after:bg-gradient-to-r after:from-transparent after:via-white/50 after:to-transparent dark:after:via-white/10",
        className
      )}
      {...props}
    />
  )
}

// 2. Text Line Variant
function SkeletonText({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <Skeleton
      className={cn("h-4 w-full rounded", className)}
      {...props}
    />
  )
}

// 3. Avatar Variant (Circular)
function SkeletonAvatar({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <Skeleton
      className={cn("h-10 w-10 rounded-full flex-shrink-0", className)}
      {...props}
    />
  )
}

// 4. Card Variant (Full placeholder)
function SkeletonCard({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn("flex flex-col space-y-3", className)} {...props}>
      <Skeleton className="h-[125px] w-[250px] rounded-xl" />
      <div className="space-y-2">
        <SkeletonText className="h-4 w-[250px]" />
        <SkeletonText className="h-4 w-[200px]" />
      </div>
    </div>
  )
}

export { Skeleton, SkeletonText, SkeletonAvatar, SkeletonCard }