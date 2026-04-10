// src/components/AnimatedLogo.jsx
import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Circle, BarChart2, Activity } from 'lucide-react';

export default function AnimatedLogo() {
  const [phase, setPhase] = useState(0);

  useEffect(() => {
    // Inyección dinámica de la tipografía Space Grotesk
    if (!document.getElementById('space-grotesk')) {
      const link = document.createElement('link');
      link.id = 'space-grotesk';
      link.href = 'https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@600;700&display=swap';
      link.rel = 'stylesheet';
      document.head.appendChild(link);
    }

    // Cronograma acelerado: De 3.2s a 1.5s aprox.
    const timers = [
      setTimeout(() => setPhase(1), 300),   // Círculo -> Barras
      setTimeout(() => setPhase(2), 600),   // Barras -> Actividad
      setTimeout(() => setPhase(3), 900),   // Actividad -> "P" Esmeralda
      setTimeout(() => setPhase(4), 1300),  // Revelado "A C I O L I"
    ];
    
    return () => timers.forEach(clearTimeout);
  }, []);

  // Configuración de métricas perfectas
  const letterSize = "96px"; 
  const letterGap = "1.8rem"; 

  // Variante de desvanecido natural (sin escala, solo opacidad)
  const fadeVariants = {
    initial: { opacity: 0 },
    animate: { opacity: 1, transition: { duration: 0.35, ease: "easeOut" } },
    exit: { opacity: 0, transition: { duration: 0.2 } },
  };

  return (
    <div className="flex flex-col items-center justify-center mb-10 w-full select-none">
      <motion.div 
        layout 
        className="flex items-center" 
        style={{ gap: letterGap }}
      >
        
        {/* BLOQUE "P" (Metamorfosis de 3 fases) */}
        <div 
          className="flex items-center justify-center relative" 
          style={{ width: '58px', height: letterSize }} 
        >
          <AnimatePresence mode="wait">
            {phase < 3 && (
              <motion.div 
                key={phase} 
                variants={fadeVariants} 
                initial="initial" 
                animate="animate" 
                exit="exit" 
                className="flex items-center justify-center"
              >
                {phase === 0 && <Circle size={64} color="var(--color-vault-text3)" strokeWidth={2} />}
                {phase === 1 && <BarChart2 size={64} color="var(--color-vault-text2)" strokeWidth={2} />}
                {phase === 2 && <Activity size={64} color="var(--color-vault-green-soft)" strokeWidth={2} />}
              </motion.div>
            )}

            {phase >= 3 && (
              <motion.span
                key="letter-p"
                variants={fadeVariants}
                initial="initial"
                animate="animate"
                className="font-bold leading-none flex items-center justify-center absolute"
                style={{ 
                  color: 'var(--color-vault-orange)', 
                  fontFamily: "'Space Grotesk', sans-serif", 
                  fontSize: letterSize,
                  textShadow: '0 0 45px rgba(234,106,26,0.5)',
                  transform: 'translateY(-3px)' // Alineación óptica de línea base
                }}
              >
                P
              </motion.span>
            )}
          </AnimatePresence>
        </div>

        {/* BLOQUE "A C I O L I" (Desvanecido secuencial perfecto) */}
        <AnimatePresence>
          {phase === 4 && (
            <motion.div 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.5 }}
              className="flex items-center" 
              style={{ gap: letterGap }}
            >
              {["A", "C", "I", "O", "L", "I"].map((letter, index) => (
                <motion.span
                  key={index}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: index * 0.08, duration: 0.4, ease: "easeOut" }}
                  className="font-bold text-white leading-none"
                  style={{ 
                    fontFamily: "'Space Grotesk', sans-serif", 
                    fontSize: letterSize,
                    textShadow: '0 0 20px rgba(255,255,255,0.08)'
                  }}
                >
                  {letter}
                </motion.span>
              ))}
            </motion.div>
          )}
        </AnimatePresence>
      </motion.div>

      {/* Subtítulo */}
      <motion.p
        initial={{ opacity: 0 }}
        animate={{ opacity: phase === 4 ? 1 : 0 }}
        transition={{ delay: 0.3 }}
        className="text-[var(--color-vault-orange-soft)] text-sm font-semibold uppercase tracking-[0.5em] mt-8"
      >
        Reconciliation System
      </motion.p>
    </div>
  );
}