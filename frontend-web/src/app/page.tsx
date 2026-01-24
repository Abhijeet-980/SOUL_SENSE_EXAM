import Link from 'next/link';

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 dark:from-gray-900 dark:to-gray-800">
      <main className="container mx-auto px-4 py-16">
        <div className="text-center">
          <h1 className="text-5xl font-bold text-gray-900 dark:text-white mb-6">
            🧠 Soul Sense
          </h1>
          <p className="text-xl text-gray-600 dark:text-gray-300 mb-8 max-w-2xl mx-auto">
            Comprehensive Emotional Intelligence Assessment Platform. Discover
            your EQ potential through scientifically-backed testing.
          </p>

          <div className="flex flex-col sm:flex-row gap-4 justify-center mb-12">
            <Link
              href="/assessment"
              className="bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-8 rounded-lg transition-colors"
            >
              Start Assessment
            </Link>
            <Link
              href="/about"
              className="bg-white hover:bg-gray-50 text-gray-900 font-semibold py-3 px-8 rounded-lg border border-gray-300 transition-colors"
            >
              Learn More
            </Link>
          </div>

          <div className="grid md:grid-cols-3 gap-8 max-w-4xl mx-auto">
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-md">
              <div className="text-3xl mb-4">📊</div>
              <h3 className="text-xl font-semibold mb-2 text-gray-900 dark:text-white">
                Comprehensive Analysis
              </h3>
              <p className="text-gray-600 dark:text-gray-300">
                Detailed emotional intelligence assessment with personalized
                insights.
              </p>
            </div>

            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-md">
              <div className="text-3xl mb-4">🎯</div>
              <h3 className="text-xl font-semibold mb-2 text-gray-900 dark:text-white">
                Actionable Results
              </h3>
              <p className="text-gray-600 dark:text-gray-300">
                Get specific recommendations to improve your emotional
                intelligence.
              </p>
            </div>

            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-md">
              <div className="text-3xl mb-4">🔒</div>
              <h3 className="text-xl font-semibold mb-2 text-gray-900 dark:text-white">
                Privacy First
              </h3>
              <p className="text-gray-600 dark:text-gray-300">
                Your data is secure and confidential. Results are for personal
                growth only.
              </p>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
