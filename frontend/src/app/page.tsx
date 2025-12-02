"use client"

import { useEffect, useState } from 'react';
import { getPastRankings, getFutureRankings, getAllPlayers, predictPlayer } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { PlayerSearch } from "@/components/ui/player-search";

export default function Dashboard() {
  const CURRENT_WEEK = 11; // You can make this dynamic later
  const [pastRankings, setPastRankings] = useState<any[]>([]);
  const [futureRankings, setFutureRankings] = useState<any[]>([]);
  const [allPlayers, setAllPlayers] = useState<any[]>([]);
  
  // Prediction State
  const [selectedPlayer, setSelectedPlayer] = useState("");
  const [predictionResult, setPredictionResult] = useState<any>(null);

  useEffect(() => {
    // Load initial data
    getPastRankings(CURRENT_WEEK - 1).then(setPastRankings);
    getFutureRankings(CURRENT_WEEK).then(setFutureRankings);
    getAllPlayers().then(setAllPlayers);
  }, []);

  const handlePredict = async () => {
    if (!selectedPlayer) return;
    const result = await predictPlayer(selectedPlayer, CURRENT_WEEK);
    setPredictionResult(result);
  };

  return (
    <div className="min-h-screen bg-slate-50 p-6 flex gap-6 justify-center">
      
      {/* --- LEFT SIDEBAR: Past Performance --- */}
      <Card className="w-80 h-fit shadow-md hidden xl:block">
        <CardHeader className="bg-slate-100 rounded-t-lg">
          <CardTitle className="text-lg">🏆 Top Performers (Wk {CURRENT_WEEK - 1})</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {pastRankings.map((p, i) => (
            <div key={i} className="flex justify-between items-center p-3 border-b text-sm hover:bg-slate-50">
              <div>
                <div className="font-bold">{p.player_name}</div>
                <div className="text-xs text-slate-500">{p.position} • {p.team}</div>
              </div>
              <div className="text-right">
                <div className="font-bold text-green-600">{p.actual_points}</div>
                <div className="text-xs text-slate-400">Proj: {p.predicted_points}</div>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>

      {/* --- CENTER: Main Action Area --- */}
      <div className="flex-1 max-w-2xl space-y-6">
        <div className="text-center mb-8">
          <h1 className="text-4xl font-extrabold text-slate-900 tracking-tight">Football AI</h1>
          <p className="text-slate-500 mt-2">Professional-Grade Multi-Model Prediction Engine</p>
        </div>

        <Card className="shadow-lg border-t-4 border-t-blue-600">
          <CardContent className="p-6">
            <Tabs defaultValue="predict" className="w-full">
              <TabsList className="grid w-full grid-cols-2 mb-6">
                <TabsTrigger value="predict">Single Player</TabsTrigger>
                <TabsTrigger value="compare">Compare (Coming Soon)</TabsTrigger>
              </TabsList>
              
              <TabsContent value="predict" className="space-y-4">
                <label className="text-sm font-medium">Select Player</label>
                <div className="flex gap-2">
                  <div className="flex-1">
                    <PlayerSearch players={allPlayers} onSelect={setSelectedPlayer} />
                  </div>
                  <Button onClick={handlePredict} className="bg-blue-600 hover:bg-blue-700">
                    Run Model
                  </Button>
                </div>

                {/* Prediction Result Display */}
                {predictionResult && (
                  <div className="mt-8 p-6 bg-slate-50 rounded-lg border border-slate-200 animate-in fade-in slide-in-from-bottom-4">
                    <div className="flex justify-between items-start">
                      <div>
                        <h2 className="text-2xl font-bold text-slate-800">{predictionResult.player_name}</h2>
                        <div className="flex gap-2 mt-2">
                          <Badge variant="secondary">{predictionResult.position}</Badge>
                          <Badge variant="outline">vs {predictionResult.opponent}</Badge>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-3xl font-black text-blue-600">
                          {predictionResult.position_specific_prediction.predicted_points}
                        </div>
                        <div className="text-sm text-slate-500">Predicted Points</div>
                      </div>
                    </div>

                    <div className="mt-6 pt-6 border-t border-slate-200">
                      <h3 className="text-sm font-semibold text-slate-900 mb-2">Ecosystem Context (Meta-Model)</h3>
                      <div className="bg-white p-4 rounded border border-slate-200 shadow-sm">
                         <div className="flex justify-between items-center">
                            <span className="text-slate-600">Adjusted Projection</span>
                            <span className="font-bold text-lg">
                              {predictionResult.ecosystem_aware_prediction?.predicted_points || "N/A"}
                            </span>
                         </div>
                         <p className="text-xs text-slate-400 mt-1">
                           {predictionResult.ecosystem_aware_prediction?.context_message}
                         </p>
                      </div>
                    </div>
                  </div>
                )}

              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>
      </div>

      {/* --- RIGHT SIDEBAR: Future Rankings --- */}
      <Card className="w-80 h-fit shadow-md hidden xl:block">
        <CardHeader className="bg-slate-100 rounded-t-lg">
          <CardTitle className="text-lg">🚀 Week {CURRENT_WEEK} Projections</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {futureRankings.map((p, i) => (
            <div key={i} className="flex justify-between items-center p-3 border-b text-sm hover:bg-slate-50">
              <div className="flex items-center gap-3">
                <span className="font-mono text-slate-400 w-4">{i+1}</span>
                <div>
                  <div className="font-bold">{p.player_name}</div>
                  <div className="text-xs text-slate-500">{p.position} vs {p.opponent}</div>
                </div>
              </div>
              <div className="font-bold text-blue-600">{p.predicted_points}</div>
            </div>
          ))}
        </CardContent>
      </Card>

    </div>
  );
}