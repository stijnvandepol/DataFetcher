#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
#  update.sh  –  DataFetcher updater
#  Gebruik: ./update.sh
# ─────────────────────────────────────────────────────────────
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_URL="https://github.com/stijnvandepol/DataFetcher"
COMPOSE="docker compose"

# ── Kleuren ───────────────────────────────────────────────────
RED='\033[0;31m'
GRN='\033[0;32m'
YLW='\033[1;33m'
BLU='\033[1;34m'
CYN='\033[0;36m'
BOLD='\033[1m'
RST='\033[0m'

hr()  { printf "${BLU}%s${RST}\n" "────────────────────────────────────────────────"; }
ok()  { printf "${GRN}✔  %s${RST}\n" "$*"; }
inf() { printf "${CYN}ℹ  %s${RST}\n" "$*"; }
warn(){ printf "${YLW}⚠  %s${RST}\n" "$*"; }
err() { printf "${RED}✖  %s${RST}\n" "$*" >&2; }

# ── Header ────────────────────────────────────────────────────
clear
hr
printf " ${BOLD}DataFetcher – Update Script${RST}\n"
printf " %s\n" "$REPO_URL"
hr
echo

# ── Vereisten checken ─────────────────────────────────────────
for cmd in git docker; do
    if ! command -v "$cmd" &>/dev/null; then
        err "'$cmd' is niet geïnstalleerd of niet in PATH."
        exit 1
    fi
done
ok "git en docker gevonden"

# ── Data-keuze ────────────────────────────────────────────────
echo
printf "${BOLD}Wil je de huidige databasedata bewaren of verwijderen?${RST}\n"
echo
printf "  ${GRN}[1]${RST}  Bewaren  – containers worden herstart, data blijft intact\n"
printf "  ${RED}[2]${RST}  Verwijderen  – PostgreSQL volume wordt gewist (nieuwe schone DB)\n"
echo
read -rp "Keuze (1/2): " DATA_CHOICE

case "$DATA_CHOICE" in
    1)
        WIPE_DATA=false
        ok "Data wordt bewaard."
        ;;
    2)
        WIPE_DATA=true
        echo
        warn "Dit verwijdert ALLE data in de database permanent."
        read -rp "Typ 'ja' om te bevestigen: " CONFIRM
        if [[ "$CONFIRM" != "ja" ]]; then
            inf "Geannuleerd. Geen wijzigingen gemaakt."
            exit 0
        fi
        warn "Data wordt verwijderd na de update."
        ;;
    *)
        err "Ongeldige keuze. Afgebroken."
        exit 1
        ;;
esac

# ── Naar repo-map ─────────────────────────────────────────────
cd "$REPO_DIR"
echo
hr
inf "Werkmap: $REPO_DIR"

# ── Git pull ──────────────────────────────────────────────────
echo
inf "Nieuwste code ophalen van GitHub..."

# Controleer of er uncommitted wijzigingen zijn
if ! git diff --quiet || ! git diff --cached --quiet; then
    warn "Er zijn lokale wijzigingen. Die worden opgeslagen (git stash)."
    git stash push -m "auto-stash door update.sh $(date +%Y%m%d-%H%M%S)"
    STASHED=true
else
    STASHED=false
fi

BEFORE=$(git rev-parse HEAD)
git pull --rebase origin "$(git branch --show-current)"
AFTER=$(git rev-parse HEAD)

if [[ "$BEFORE" != "$AFTER" ]]; then
    ok "Code bijgewerkt: ${BEFORE:0:7} → ${AFTER:0:7}"
    echo
    inf "Wijzigingen:"
    git log --oneline "${BEFORE}..${AFTER}" | sed 's/^/   /'
else
    ok "Geen nieuwe commits – code was al up-to-date."
fi

if [[ "$STASHED" == "true" ]]; then
    inf "Lokale wijzigingen terugplaatsen (git stash pop)..."
    git stash pop || warn "Stash pop mislukt – controleer 'git stash list' handmatig."
fi

# ── Containers stoppen ────────────────────────────────────────
echo
hr
inf "Containers stoppen..."
$COMPOSE down
ok "Containers gestopt."

# ── Data verwijderen (optioneel) ──────────────────────────────
if [[ "$WIPE_DATA" == "true" ]]; then
    echo
    inf "PostgreSQL volume verwijderen..."
    # Volume naam = <project>_pgdata (project = directory naam in lowercase)
    PROJECT_NAME=$(basename "$REPO_DIR" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9_-')
    VOLUME_NAME="${PROJECT_NAME}_pgdata"

    if docker volume inspect "$VOLUME_NAME" &>/dev/null; then
        docker volume rm "$VOLUME_NAME"
        ok "Volume '$VOLUME_NAME' verwijderd."
    else
        # Probeer ook fetcher_temp mee
        warn "Volume '$VOLUME_NAME' niet gevonden – misschien andere projectnaam."
        inf "Beschikbare volumes met 'pgdata':"
        docker volume ls --filter name=pgdata --format "  {{.Name}}"
        read -rp "Voer de exacte volumenaam in (of Enter om over te slaan): " MANUAL_VOL
        if [[ -n "$MANUAL_VOL" ]]; then
            docker volume rm "$MANUAL_VOL"
            ok "Volume '$MANUAL_VOL' verwijderd."
        else
            warn "Volume overgeslagen. Data is mogelijk niet gewist."
        fi
    fi

    # Verwijder ook fetcher temp volume
    FETCHER_VOL="${PROJECT_NAME}_fetcher_temp"
    if docker volume inspect "$FETCHER_VOL" &>/dev/null; then
        docker volume rm "$FETCHER_VOL"
        ok "Volume '$FETCHER_VOL' verwijderd."
    fi
fi

# ── Opnieuw bouwen en starten ─────────────────────────────────
echo
hr
inf "Containers opnieuw bouwen en starten..."
$COMPOSE up -d --build
ok "Alle containers draaien."

# ── Status tonen ──────────────────────────────────────────────
echo
hr
$COMPOSE ps
echo
hr
if [[ "$WIPE_DATA" == "true" ]]; then
    printf "${YLW}  Database is leeg. Start een handmatige fetch via het admin panel.${RST}\n"
else
    printf "${GRN}  Update klaar. Data is bewaard.${RST}\n"
fi
hr
