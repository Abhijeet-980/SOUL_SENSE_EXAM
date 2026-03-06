import { NextRequest, NextResponse } from 'next/server';

const BACKEND_URL = process.env.BACKEND_URL || 'http://127.0.0.1:8000';

export async function GET(request: NextRequest) {
  const username = request.nextUrl.searchParams.get('username');
  
  if (!username) {
    return NextResponse.json({ error: 'Username required' }, { status: 400 });
  }

  try {
    const response = await fetch(
      `${BACKEND_URL}/api/v1/auth/check-username?username=${encodeURIComponent(username)}`,
      { headers: { 'Accept': 'application/json' } }
    );
    const data = await response.json();
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('Backend proxy error:', error);
    return NextResponse.json({ available: true, message: 'Could not verify' }, { status: 200 });
  }
}
