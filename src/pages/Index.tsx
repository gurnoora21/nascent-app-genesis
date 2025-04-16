
import React from 'react';
import { Button } from '@/components/ui/button';

const Index: React.FC = () => {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-soft-purple to-soft-blue">
      <div className="text-center max-w-xl p-8 bg-white/10 backdrop-blur-md rounded-xl shadow-2xl">
        <h1 className="text-5xl font-bold mb-6 text-dark-purple">
          Welcome to Your Blank App
        </h1>
        <p className="text-xl text-slate-700 mb-8">
          This is your canvas. Start building something amazing!
        </p>
        <Button variant="outline" className="text-lg px-8 py-3 hover:bg-soft-purple/10">
          Get Started
        </Button>
      </div>
    </div>
  );
};

export default Index;
