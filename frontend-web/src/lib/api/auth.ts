import { z } from 'zod';
import { PasswordResetComplete } from '../validation/schemas';
import { ApiError } from './errors';

const API_Base = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1';

export const authApi = {
  async initiatePasswordReset(email: string): Promise<{ message: string }> {
    const response = await fetch(`${API_Base}/auth/password-reset/initiate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email }),
    });

    if (!response.ok) {
      let errorData;
      try {
        errorData = await response.json();
      } catch {
        errorData = { message: 'Network error or invalid JSON response' };
      }
      throw new ApiError(response.status, errorData);
    }

    return response.json();
  },

  async completePasswordReset(data: {
    email: string;
    otp_code: string;
    new_password: string;
  }): Promise<{ message: string }> {
    const response = await fetch(`${API_Base}/auth/password-reset/complete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });

    if (!response.ok) {
      let errorData;
      try {
        errorData = await response.json();
      } catch {
        errorData = { message: 'Network error or invalid JSON response' };
      }
      throw new ApiError(response.status, errorData);
    }

    return response.json();
  },
};
