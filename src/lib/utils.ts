
import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// Format function for Spotify release dates
export function formatSpotifyReleaseDate(releaseDate: string | null): string | null {
  if (!releaseDate) return null;
  
  const dateParts = releaseDate.split('-');
  
  if (dateParts.length === 1 && dateParts[0].length === 4) {
    // Year only format (e.g., "2012") - append "-01-01" to make it January 1st
    return `${releaseDate}-01-01`;
  } else if (dateParts.length === 2) {
    // Year-month format (e.g., "2012-03") - append "-01" to make it the 1st day of the month
    return `${releaseDate}-01`;
  } else if (dateParts.length === 3) {
    // Already full date format, return as is
    return releaseDate;
  } else {
    // Unknown format, log it and return null
    console.error(`Unknown release date format: ${releaseDate}`);
    return null;
  }
}

// Calculate backoff time with jitter
export function calculateBackoffMs(retryCount: number, baseDelay = 1000): number {
  // Exponential backoff: 1s, 2s, 4s, 8s, 16s...
  const exponentialDelay = baseDelay * Math.pow(2, retryCount);
  
  // Add random jitter (±25%)
  const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
  
  return Math.min(exponentialDelay + jitter, 30000); // Cap at 30 seconds
}

// Format a number with commas
export function formatNumber(num: number): string {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Format a date for display
export function formatDate(date: string | Date): string {
  if (!date) return '';
  const d = new Date(date);
  return d.toLocaleDateString();
}

// Format a timestamp with date and time
export function formatTimestamp(date: string | Date): string {
  if (!date) return '';
  const d = new Date(date);
  return d.toLocaleString();
}

// Calculate percentage
export function calculatePercentage(value: number, total: number): number {
  if (total === 0) return 0;
  return Math.round((value / total) * 100);
}

// Format a duration in milliseconds to a readable string
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${Math.round(ms / 1000)}s`;
  const minutes = Math.floor(ms / 60000);
  const seconds = Math.floor((ms % 60000) / 1000);
  return `${minutes}m ${seconds}s`;
}

// Truncate string with ellipsis
export function truncateString(str: string, maxLength: number): string {
  if (!str) return '';
  if (str.length <= maxLength) return str;
  return str.substring(0, maxLength) + '...';
}

// Convert snake_case to Title Case
export function snakeToTitleCase(str: string): string {
  if (!str) return '';
  return str
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

// Sleep for a specified number of milliseconds
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Generate a correlation ID
export function generateCorrelationId(): string {
  return `${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;
}

// Handle API response errors
export function handleApiError(error: any): { message: string; status?: number } {
  if (error.response) {
    return {
      message: error.response.data?.message || error.message,
      status: error.response.status
    };
  }
  return { message: error.message };
}
