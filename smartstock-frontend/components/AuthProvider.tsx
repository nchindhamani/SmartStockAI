'use client';

import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { API_BASE_URL, TOKEN_KEY, USER_KEY, apiFetch } from '@/lib/api';

export interface AuthUser {
  id: string;
  email: string;
}

interface AuthContextValue {
  user: AuthUser | null;
  token: string | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

function persistSession(token: string, user: AuthUser) {
  sessionStorage.setItem(TOKEN_KEY, token);
  sessionStorage.setItem(USER_KEY, JSON.stringify(user));
}

function clearSession() {
  sessionStorage.removeItem(TOKEN_KEY);
  sessionStorage.removeItem(USER_KEY);
}

function parseError(payload: unknown, fallback: string): string {
  if (
    payload &&
    typeof payload === 'object' &&
    'error' in payload &&
    payload.error &&
    typeof payload.error === 'object' &&
    'message' in payload.error
  ) {
    return String((payload.error as { message: string }).message);
  }
  if (payload && typeof payload === 'object' && 'detail' in payload) {
    return String((payload as { detail: string }).detail);
  }
  return fallback;
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const storedToken = sessionStorage.getItem(TOKEN_KEY);
    if (!storedToken) {
      setLoading(false);
      return;
    }

    (async () => {
      try {
        const response = await apiFetch('/api/auth/me', { token: storedToken });
        if (!response.ok) {
          clearSession();
          setUser(null);
          setToken(null);
          return;
        }
        const me: AuthUser = await response.json();
        setToken(storedToken);
        setUser(me);
        persistSession(storedToken, me);
      } catch {
        clearSession();
        setUser(null);
        setToken(null);
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const authenticate = useCallback(async (path: string, email: string, password: string) => {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      throw new Error(parseError(payload, 'Authentication failed'));
    }
    persistSession(payload.access_token, payload.user);
    setToken(payload.access_token);
    setUser(payload.user);
  }, []);

  const login = useCallback(
    (email: string, password: string) => authenticate('/api/auth/login', email, password),
    [authenticate]
  );

  const register = useCallback(
    (email: string, password: string) => authenticate('/api/auth/register', email, password),
    [authenticate]
  );

  const logout = useCallback(async () => {
    try {
      await apiFetch('/api/auth/logout', { method: 'POST', token });
    } finally {
      clearSession();
      setUser(null);
      setToken(null);
    }
  }, [token]);

  const value = useMemo(
    () => ({ user, token, loading, login, register, logout }),
    [user, token, loading, login, register, logout]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return ctx;
}
