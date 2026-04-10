// src/pages/Login/index.jsx
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useForm } from 'react-hook-form';
import { z } from 'zod';
import { zodResolver } from '@hookform/resolvers/zod';
import { useAuthStore } from '../../stores/auth.store.js';
import { loginRequest } from '../../api/endpoints/auth.api.js';
import { motion } from 'framer-motion';
import AnimatedLogo from '../../components/AnimatedLogo.jsx';

const loginSchema = z.object({
  username: z.string().min(1, 'Username is required'),
  password: z.string().min(1, 'Password is required'),
});

export default function LoginPage() {
  const navigate  = useNavigate();
  const setAuth   = useAuthStore(state => state.setAuth);
  const [error, setError] = useState(null);

  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
  } = useForm({
    resolver: zodResolver(loginSchema),
  });

  async function onSubmit(data) {
    try {
      setError(null);
      const response = await loginRequest(data);
      setAuth({
        token: response.data.accessToken,
        user:  response.data.user,
      });
      navigate('/');
    } catch (err) {
      const message = err.response?.data?.message || 'Access denied. Invalid credentials.';
      setError(message);
    }
  }

  return (
    <div className="login-page-root min-h-screen flex flex-col items-center justify-center p-6 bg-[#0D0F0D] selection:bg-[var(--color-vault-orange)]/30 selection:text-white relative overflow-hidden">
      
      {/* Dynamic atmospheric glow — Solar Orange */}
      <div className="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] bg-[var(--color-vault-orange)] rounded-full blur-[120px] opacity-[0.03] pointer-events-none" />
      <div className="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] bg-[var(--color-vault-orange)] rounded-full blur-[120px] opacity-[0.03] pointer-events-none" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-[var(--color-vault-orange)] rounded-full blur-[160px] opacity-[0.05] pointer-events-none" />

      <div className="w-full max-w-[440px] z-10 flex flex-col items-center">
        
        <AnimatedLogo />

        <motion.div 
          initial={{ opacity: 0, y: 40, scale: 0.98 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          transition={{ 
            delay: 1.6, 
            duration: 0.8, 
            type: "spring",
            stiffness: 100,
            damping: 20
          }} 
          className="w-full bg-[var(--color-vault-surface)]/60 backdrop-blur-xl border border-[var(--color-vault-border)] rounded-2xl p-8 shadow-[0_25px_70px_rgba(0,0,0,0.7)] relative overflow-hidden"
        >
          {/* Top scanning line effect — Solar Orange */}
          <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-[var(--color-vault-orange)]/40 to-transparent" />

          <h2 className="text-xl font-semibold text-[var(--color-vault-text)] mb-8 text-center tracking-tight">
            Secure Authentication
          </h2>

          {error && (
            <motion.div 
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              className="bg-[var(--color-vault-red)]/10 border border-[var(--color-vault-red)]/20 text-[var(--color-vault-red)] rounded-lg px-4 py-3 mb-6 text-sm flex items-center gap-3"
            >
              <div className="w-1 h-1 rounded-full bg-[var(--color-vault-red)] shadow-[0_0_8px_var(--color-vault-red)] shrink-0" />
              {error}
            </motion.div>
          )}

          <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
            <div className="group">
              <label className="block text-[10px] font-bold text-[var(--color-vault-text3)] mb-2 uppercase tracking-[0.2em] group-focus-within:text-[var(--color-vault-orange-soft)] transition-colors">
                Verification Identity
              </label>
              <input
                {...register('username')}
                type="text"
                placeholder="User identifier"
                autoComplete="username"
                className="w-full bg-[#080A08]/80 border border-[var(--color-vault-border)] text-[var(--color-vault-text)] 
                           placeholder-[var(--color-vault-text3)]/50 rounded-lg px-4 py-3.5 text-sm
                           focus:outline-none focus:ring-1 focus:ring-[var(--color-vault-orange)]/50 
                           focus:border-[var(--color-vault-orange)]/50 transition-all shadow-2xl"
              />
              {errors.username && (
                <p className="text-[var(--color-vault-red)] text-[10px] mt-2 font-medium uppercase tracking-wider">{errors.username.message}</p>
              )}
            </div>

            <div className="group">
              <label className="block text-[10px] font-bold text-[var(--color-vault-text3)] mb-2 uppercase tracking-[0.2em] group-focus-within:text-[var(--color-vault-orange-soft)] transition-colors">
                Security Key
              </label>
              <input
                {...register('password')}
                type="password"
                placeholder="••••••••"
                autoComplete="current-password"
                className="w-full bg-[#080A08]/80 border border-[var(--color-vault-border)] text-[var(--color-vault-text)] 
                           placeholder-[var(--color-vault-text3)]/50 rounded-lg px-4 py-3.5 text-sm
                           focus:outline-none focus:ring-1 focus:ring-[var(--color-vault-orange)]/50 
                           focus:border-[var(--color-vault-orange)]/50 transition-all shadow-2xl"
              />
              {errors.password && (
                <p className="text-[var(--color-vault-red)] text-[10px] mt-2 font-medium uppercase tracking-wider">{errors.password.message}</p>
              )}
            </div>

            <motion.button
              whileHover={{ scale: 1.01 }}
              whileTap={{ scale: 0.99 }}
              type="submit"
              disabled={isSubmitting}
              className="w-full bg-[var(--color-vault-orange)] hover:bg-[var(--color-vault-orange-soft)] disabled:bg-[var(--color-vault-orange)]/50 
                         disabled:cursor-not-allowed text-[var(--color-vault-text-inv)] font-bold tracking-[0.15em] uppercase
                         rounded-lg px-4 py-4 text-xs transition-all duration-300 mt-4
                         shadow-[0_0_20px_rgba(234,106,26,0.2)] hover:shadow-[0_0_35px_rgba(234,106,26,0.4)]"
            >
              {isSubmitting ? 'Authorizing...' : 'Authorize'}
            </motion.button>
          </form>
        </motion.div>

        {/* The Signature Footer (Watermark Style) */}
        <motion.p 
          initial={{ opacity: 0 }}
          animate={{ opacity: 0.15 }}
          transition={{ delay: 2.2 }}
          className="text-center text-white text-[8px] mt-12 font-medium tracking-[0.3em] uppercase select-none"
        >
          Powered by Pacioli Core — F.A. Pilatasig
        </motion.p>

      </div>
    </div>
  );
}