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

# ── Container-selectie ───────────────────────────────────────
echo
printf "${BOLD}Welke containers wil je updaten?${RST}\n"
echo
printf "  ${GRN}[1]${RST}  Alle containers\n"
printf "  ${GRN}[2]${RST}  Alleen fetcher (postgres moet draaien)\n"
printf "  ${GRN}[3]${RST}  Alleen web (postgres + fetcher moeten draaien)\n"
printf "  ${GRN}[4]${RST}  Alleen nginx (geen dependencies)\n"
printf "  ${GRN}[5]${RST}  Web + nginx (postgres + fetcher moeten draaien)\n"
printf "  ${GRN}[6]${RST}  Fetcher + web (postgres moet draaien, fetcher wordt eerst bijgewerkt)\n"
echo
read -rp "Keuze (1-6): " CONTAINER_CHOICE

SELECTED_SERVICES=()
REQUIRED_RUNNING=()

case "$CONTAINER_CHOICE" in
    1)
        SELECTED_SERVICES=("fetcher" "web" "nginx")
        REQUIRED_RUNNING=()
        ok "Alle containers worden bijgewerkt."
        ;;
    2)
        SELECTED_SERVICES=("fetcher")
        REQUIRED_RUNNING=("postgres")
        ok "Alleen fetcher wordt bijgewerkt (postgres blijft draaien)."
        ;;
    3)
        SELECTED_SERVICES=("web")
        REQUIRED_RUNNING=("postgres" "fetcher")
        ok "Alleen web wordt bijgewerkt (postgres + fetcher blijven draaien)."
        ;;
    4)
        SELECTED_SERVICES=("nginx")
        REQUIRED_RUNNING=("postgres" "fetcher" "web")
        ok "Alleen nginx wordt bijgewerkt (geen dependencies)."
        ;;
    5)
        SELECTED_SERVICES=("web" "nginx")
        REQUIRED_RUNNING=("postgres" "fetcher")
        ok "Web + nginx worden bijgewerkt (postgres + fetcher blijven draaien)."
        ;;
    6)
        SELECTED_SERVICES=("fetcher" "web")
        REQUIRED_RUNNING=("postgres")
        ok "Fetcher en web worden bijgewerkt (postgres blijft draaien, fetcher eerst)."
        ;;
    *)
        err "Ongeldige keuze. Afgebroken."
        exit 1
        ;;
esac

# ── Controleer of benodigde services draaien ───────────────────
if [[ ${#REQUIRED_RUNNING[@]} -gt 0 ]]; then
    echo
    inf "Controleer of vereiste services draaien..."
    for service in "${REQUIRED_RUNNING[@]}"; do
        has_container=$($COMPOSE ps $service 2>/dev/null | grep -c "$service" || echo 0)
        if [[ "$has_container" -eq 0 ]]; then
            err "Service '$service' is niet beschikbaar, maar is vereist voor deze update."
            err "Start alle containers eerst: 'docker compose up -d'"
            exit 1
        fi
    done
    ok "Alle vereiste services zijn beschikbaar."
fi

# ── Data-keuze ────────────────────────────────────────────────
WIPE_DATA=false
if [[ ${#SELECTED_SERVICES[@]} -eq 3 ]] || ([[ " ${SELECTED_SERVICES[@]} " =~ "fetcher" ]] && [[ " ${SELECTED_SERVICES[@]} " =~ "web" ]]); then
    # Alleen data wissen als fetcher en web beide worden bijgewerkt
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
else
    inf "Data-wipe is alleen beschikbaar bij volledige updates of fetcher+web. Data wordt bewaard."
fi

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
inf "Geselecteerde containers stoppen..."
$COMPOSE stop "${SELECTED_SERVICES[@]}"
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
inf "Geselecteerde containers opnieuw bouwen en starten..."

# Voor opatie 6 (fetcher + web): fetcher eerst opnieuw starten zodat web kan wachten
if [[ " ${SELECTED_SERVICES[@]} " =~ "fetcher" ]] && [[ " ${SELECTED_SERVICES[@]} " =~ "web" ]]; then
    inf "Fetcher eerst opnieuw starten (web depended hiervan)..."
    $COMPOSE up -d --build fetcher
    ok "Fetcher opnieuw gestart."
    sleep 3
    inf "Vervolgens web opnieuw starten..."
    $COMPOSE up -d --build web
    ok "Web opnieuw gestart."
else
    $COMPOSE up -d --build "${SELECTED_SERVICES[@]}"
    ok "Geselecteerde containers opnieuw gestart."
fi

# ── Status tonen ──────────────────────────────────────────────
echo
hr
printf "${BOLD}Status van alle containers:${RST}\n"
$COMPOSE ps
echo
hr
if [[ "$WIPE_DATA" == "true" ]]; then
    printf "${YLW}  Database is leeg. Start een handmatige fetch via het admin panel.${RST}\n"
else
    printf "${GRN}  Update klaar. Bijgewerkte services: ${SELECTED_SERVICES[*]}${RST}\n"
fi
if [[ ${#SELECTED_SERVICES[@]} -lt 3 ]]; then
    printf "${CYN}  Opmerking: Dependencies in docker-compose:${RST}\n"
    printf "${CYN}    • fetcher: requires postgres${RST}\n"
    printf "${CYN}    • web: requires postgres + fetcher${RST}\n"
    printf "${CYN}    • nginx: onafhankelijk${RST}\n"
    printf "${CYN}  Andere containers zijn ongewijzigd en draaien nog.${RST}\n"
fi
hr
