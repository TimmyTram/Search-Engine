import { useState } from "react";
import { Search, Loader2, ExternalLink } from "lucide-react";

export default function App() {
  const [query, setQuery] = useState("");

  type SearchResult = {
    url: string;
  };

  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);

  const handleSearch = (e: React.FormEvent<HTMLButtonElement> | React.KeyboardEvent<HTMLInputElement>): void => {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    const apiUrl = import.meta.env.VITE_API_URL || "http://localhost:8080";
    fetch(`${apiUrl}/api/v1/search?q=${encodeURIComponent(query)}&limit=100`)
      .then(response => {
        if (!response.ok) {
          throw new Error("Network response was not ok");
        }
        return response.json();
      })
      .then(data => {
        console.log(data.results);
        setResults(data.results || []);
        setLoading(false);
      })
      .catch(error => {
        console.error("Error fetching search results:", error);
        setLoading(false);
      });
  };

  // Function to truncate URL for display purposes
  const formatUrl = (url: string): string => {
    // Remove protocol for cleaner display
    const withoutProtocol = url.replace(/^https?:\/\//, '');
    return withoutProtocol;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 py-12 px-4">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-extrabold text-indigo-900 mb-2">Search Engine</h1>
          <p className="text-gray-600 max-w-md mx-auto">Find what you're looking for with our powerful search tool</p>
        </div>

        {/* Search Input */}
        <div className="mb-8">
          <div className="flex items-center relative">
            <input
              type="text"
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder="Enter your search query..."
              className="w-full p-4 pl-12 pr-16 rounded-xl border border-gray-300 shadow-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none text-gray-800"
              onKeyPress={e => e.key === 'Enter' && handleSearch(e)}
            />
            <div className="absolute left-4 text-gray-400">
              <Search size={20} />
            </div>
            <button
              onClick={handleSearch}
              className="absolute right-2 bg-indigo-600 hover:bg-indigo-700 text-white py-2 px-4 rounded-lg transition-colors duration-200 flex items-center justify-center"
            >
              {loading ? (
                <Loader2 size={20} className="animate-spin" />
              ) : (
                "Search"
              )}
            </button>
          </div>
        </div>

        {/* Search Results */}
        <div className="bg-white rounded-xl shadow-md p-6">
          {loading && (
            <div className="flex justify-center items-center py-12">
              <Loader2 size={32} className="animate-spin text-indigo-600" />
              <span className="ml-2 text-gray-600">Searching...</span>
            </div>
          )}

          {!loading && results.length === 0 && (
            <div className="text-center py-12 text-gray-500">
              <p>Enter a search query and press "Search" to see results</p>
            </div>
          )}

          {!loading && results.length > 0 && (
            <div>
              <h2 className="text-xl font-semibold text-gray-800 mb-4">Search Results ({results.length})</h2>
              <ul className="space-y-4">
                {results.map((result, index) => (
                  <li key={index} className="p-4 border border-gray-100 rounded-lg hover:bg-blue-50 transition-colors duration-150">
                    <a
                      href={result.url}
                      className="flex items-center text-indigo-600 hover:text-indigo-800 text-lg font-medium group"
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      <div className="truncate max-w-full">
                        {formatUrl(result.url)}
                      </div>
                      <ExternalLink size={16} className="ml-2 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity" />
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}