import React, { useState, useEffect } from "react";
import {
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  TextField,
  MenuItem,
  Grid,
  FormControl,
  InputLabel,
  Select,
} from "@mui/material";

// Importer le service API
import { getMatchs } from "../services/api";

const MatchsPage = () => {
  const [loading, setLoading] = useState(true);
  const [matchs, setMatchs] = useState([]);
  const [filteredMatchs, setFilteredMatchs] = useState([]);

  // Filtres
  const [sportFilter, setSportFilter] = useState("");
  const [statutFilter, setStatutFilter] = useState("");

  useEffect(() => {
    const fetchMatchs = async () => {
      try {
        // Récupérer les données depuis l'API
        const data = await getMatchs();
        setMatchs(data);
        setFilteredMatchs(data);
        setLoading(false);
      } catch (error) {
        console.error("Erreur lors du chargement des matchs:", error);
        setLoading(false);
      }
    };

    fetchMatchs();
  }, []);

  // Filtrer les matchs quand les filtres changent
  useEffect(() => {
    let result = [...matchs];

    if (sportFilter) {
      result = result.filter((match) => match.sport_nom === sportFilter);
    }

    if (statutFilter) {
      result = result.filter((match) => match.statut === statutFilter);
    }

    setFilteredMatchs(result);
  }, [matchs, sportFilter, statutFilter]);

  const statusLabels = {
    SCHEDULED: "prévu",
    TIMED: "programmé",
    IN_PLAY: "en cours",
    PAUSED: "en pause",
    FINISHED: "terminé",
    LIVE: "en cours",
  };

  const getStatutColor = (statut) => {
    switch (statut.toLowerCase()) {
      case "programmé":
      case "prévu":
      case "timed":
      case "scheduled":
        return "primary";
      case "en cours":
      case "in_play":
      case "live":
      case "en pause":
      case "paused":
        return "success";
      case "terminé":
      case "finished":
        return "default";
      case "reporté":
      case "postponed":
        return "warning";
      case "annulé":
      case "cancelled":
        return "error";
      default:
        return "default";
    }
  };

  // Extraire les sports uniques pour le filtre
  const uniqueSports = [...new Set(matchs.map((match) => match.sport_nom))];

  // Extraire les statuts uniques pour le filtre
  const uniqueStatuts = [...new Set(matchs.map((match) => match.statut))];

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Matchs
      </Typography>

      {/* Filtres */}
      <Box mb={3}>
        <Grid container spacing={2}>
          <Grid item xs={12} sm={6} md={3}>
            <FormControl fullWidth>
              <InputLabel>Sport</InputLabel>
              <Select
                value={sportFilter}
                label="Sport"
                onChange={(e) => setSportFilter(e.target.value)}
              >
                <MenuItem value="">Tous les sports</MenuItem>
                {uniqueSports.map((sport) => (
                  <MenuItem key={sport} value={sport}>
                    {sport}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>

          <Grid item xs={12} sm={6} md={3}>
            <FormControl fullWidth>
              <InputLabel>Statut</InputLabel>
              <Select
                value={statutFilter}
                label="Statut"
                onChange={(e) => setStatutFilter(e.target.value)}
              >
                <MenuItem value="">Tous les statuts</MenuItem>
                {uniqueStatuts.map((statut) => (
                  <MenuItem key={statut} value={statut}>
                    {statut}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
        </Grid>
      </Box>

      {loading ? (
        <Typography>Chargement des matchs...</Typography>
      ) : (
        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>ID</TableCell>
                <TableCell>Sport</TableCell>
                <TableCell>Match</TableCell>
                <TableCell>Score</TableCell>
                <TableCell>Date</TableCell>
                <TableCell>Lieu</TableCell>
                <TableCell>Statut</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredMatchs.map((match) => (
                <TableRow key={match.match_id}>
                  <TableCell>{match.match_id}</TableCell>
                  <TableCell>{match.sport_nom}</TableCell>
                  <TableCell>
                    {match.equipe_domicile} vs {match.equipe_exterieur}
                  </TableCell>
                  <TableCell>
                    {match.score_domicile} - {match.score_exterieur}
                  </TableCell>
                  <TableCell>
                    {new Date(match.date_match).toLocaleString()}
                  </TableCell>
                  <TableCell>{match.lieu}</TableCell>
                  <TableCell>
                    <Chip
                      label={match.statut}
                      color={getStatutColor(match.statut)}
                      size="small"
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
};

export default MatchsPage;
