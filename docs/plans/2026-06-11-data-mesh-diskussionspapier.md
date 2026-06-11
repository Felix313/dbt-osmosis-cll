# dbt-osmosis-cll im Data Mesh — Diskussionspapier für die zentrale Data Governance

**Datum:** 2026-06-11
**Zweck:** Abstimmungsgrundlage mit dem Governance-Team: Wie funktioniert das Tool,
wie passt es in unseren Data-Mesh-/Data-Product-Ansatz, und welche Punkte müssen
teamübergreifend vereinbart werden, bevor andere Domänen es übernehmen.
**Hinweis:** Dieses Repo ist öffentlich — das Papier ist bewusst frei von internen
System-, Schema- und Abteilungsnamen.

---

## 1. Was das Tool heute leistet

**dbt-osmosis-cll** automatisiert die Spalten-Dokumentation in dbt-Repos. Kernidee:
*Beschreibungen werden genau einmal gepflegt — am Ursprung einer Spalte — und von dort
automatisch durch die gesamte Pipeline vererbt.* Vier Bausteine:

1. **Column-Level-Lineage-Engine (CLL).** Ein SQL-Parser analysiert die kompilierten
   Modelle und bestimmt für jede Spalte, woher ihr Wert stammt — durch beliebig viele
   CTE-Ebenen, Joins, Renames, UNIONs. Jede Kante wird semantisch klassifiziert
   (durchgereicht / umbenannt / berechnet / aggregiert / Window / UNION / Literal /
   generiert). Getestet gegen den vollständigen Bestand eines realen Snowflake-Repos
   (463 Modelle, 0 Parserfehler, 99,6 % Lineage-Abdeckung).

2. **Dokumentationsvererbung mit Provenienz.** `yaml document` füllt undokumentierte
   Spalten mit der Beschreibung ihres CLL-Ursprungs und schreibt einen maschinenlesbaren
   Herkunftsverweis (`desc-source`) in die Spalten-Meta. Der Verweis wird bei jedem Lauf
   neu berechnet — er kann nicht veralten. Zusätzlich werden menschenlesbare
   Herkunfts-Annotationen an die Beschreibung angehängt („Renamed from …“, „Computed
   here from A.X, B.Y“) und auf Wunsch maschinenlesbare Meta-Tags
   (`renamed_from` / `derived_from` / `computed_in`) für Kataloge geschrieben.

3. **Doc-Health / Trust-Report.** `yaml doc-health` liefert pro Modell und projektweit
   den Dokumentationsstand als stabile JSON-Schnittstelle (CI-tauglich, mit
   `--min-coverage`-Gate). Dokumentierte Spalten werden nach **Vertrauensklasse**
   aufgeschlüsselt: *authored* (am Knoten von Menschen geschrieben), *inherited*
   (vom Ursprung vererbt, mit Provenienz), *glossary* (zentral gepflegt). So wird
   „dokumentiert“ von „vertrauenswürdig dokumentiert“ unterscheidbar, und stille
   Lineage-Regressionen fallen in CI auf.

4. **Lineage-Explorer.** Ein lokaler Read-only-Webserver (`lineage explore`)
   visualisiert die Spalten-Lineage des Repos — ohne Warehouse-Verbindung, rein aus
   den dbt-Artefakten.

Bewusste Designentscheidung: Das Tool enthält **keinen LLM-Client**. KI-gestützte
Dokumentation erfolgt durch Coding-Agents im Repo (z. B. Claude Code), die gegen die
deterministischen Schnittstellen arbeiten: doc-health zeigt die Lücken, der Mensch/Agent
dokumentiert am Ursprung, `yaml document` propagiert. Generierter Text landet dadurch
nie an Nicht-Ursprungs-Schichten, die Provenienz bleibt ehrlich.

---

## 2. Passung zum Data-Mesh-/Data-Product-Ansatz

Unser Mesh-Muster — jede Domäne hat ihr eigenes dbt-Repo und konsumiert die Data
Products anderer Domänen — bildet sich in dbt-core über **Sources** ab: Das Repo der
konsumierenden Domäne deklariert die Data Products der produzierenden Domäne als
Sources. Genau an dieses Muster ist das Tool bereits angelehnt:

- **Sources sind Ursprungs-Grenzen.** Die Lineage-Verfolgung endet bewusst an der
  Source — also an der Domänengrenze. Innerhalb des Repos gilt: Doku-Ownership am
  Ursprung, automatische Vererbung downstream.
- **Der Endpoint-Layer ist der Vertrag.** Data-Product-Modelle können so konfiguriert
  werden, dass jede Spalte ihre Herkunft menschen- und maschinenlesbar trägt
  (Annotation + Meta-Tags). Konsumenten und Kataloge lesen das direkt aus dem
  dbt-Manifest des Produzenten.
- **Föderierte Governance passt zur Architektur:** Jedes Team betreibt das Tool in
  seinem Repo autonom (eigene Konfiguration, eigene CI-Gates); die zentrale Governance
  definiert die Konventionen an den Schnittstellen.

Kurz: Das Tool ist heute **mesh-kompatibel**. Damit es **mesh-nativ** wird — d. h.
Dokumentation und Provenienz fließen über Repo-Grenzen hinweg —, braucht es die
folgenden Vereinbarungen.

---

## 3. Zusammenspiel mit Atlan

Die zentrale Governance nimmt die dbt-Manifeste als Input für die Atlan-Lineage auf.
Das ist kein Konkurrenz-, sondern ein Zulieferverhältnis — **osmosis-cll erzeugt
Inhalte im dbt-Repo, Atlan verteilt sie an die Organisation.** Alles, was das Tool
schreibt, landet im `manifest.json` und damit automatisch im bestehenden
Atlan-Ingest. Konkret greifen die Werkzeuge an vier Stellen ineinander:

1. **Beschreibungen: Docs-as-Code füllt den Katalog.** osmosis-cll dokumentiert
   Spalten in der Breite (am Ursprung gepflegt, automatisch vererbt) — Atlan zeigt
   genau diese Beschreibungen aus dem Manifest an. Ohne das Tool wären die meisten
   Spalten im Katalog leer oder müssten doppelt in Atlan gepflegt werden. Damit keine
   Drift entsteht, braucht es eine klare Schreibrichtung (→ A5).

2. **Provenienz wird in Atlan sichtbar und filterbar.** Die maschinenlesbaren
   Meta-Tags (`renamed_from` / `derived_from` / `computed_in`, `desc-source`) stehen
   im Manifest-`meta` der Spalten und können in Atlan als Custom Metadata gemappt
   werden. Ergebnis: Katalognutzer sehen pro Spalte nicht nur *was* sie bedeutet,
   sondern *woher* der Wert stammt und ob die Beschreibung am Knoten verfasst,
   vererbt oder glossarbasiert ist — Trust-Badges im Katalog (→ A6).

3. **Zwei Lineage-Sichten, zwei Aufgaben.** Atlan baut die mesh- und
   plattformweite Lineage für Discovery und Governance — die richtige Sicht für
   alle Datennutzer. Die CLL-Engine von osmosis-cll ist das **Entwicklungszeit**-
   Pendant: Sie läuft lokal/in CI *vor* dem Merge, ohne Warehouse-Verbindung, und
   treibt die Doku-Vererbung an. Der mitgelieferte Explorer bleibt das schnelle
   Engineering-Werkzeug im Repo; für organisationsweite Lineage-Fragen ist Atlan
   das Zielsystem. (Das relativiert auch den Ausbaubedarf von B4: ein föderierter
   Explorer ist nice-to-have, wenn Atlan die Mesh-Sicht ohnehin abdeckt.)

4. **Qualität vor Ingest statt Lücken im Katalog.** `doc-health` misst pro Repo,
   wie vollständig und vertrauenswürdig die Dokumentation ist — *bevor* das Manifest
   in Atlan landet. Als CI-Gate (`--min-coverage`) stellt es sicher, dass Atlan nur
   Manifeste mit vereinbartem Mindeststand erhält; das aggregierte Trust-Reporting
   (B2) liefert der Governance die zugehörige Kennzahl je Domäne.

Ein Punkt zur Gestaltung: Die menschenlesbaren Herkunfts-Annotationen („OSMOSIS →
Renamed from …") sind Teil des Beschreibungstexts und erscheinen daher auch in
Atlan. Ob das gewünscht ist (Herkunft direkt im Katalog lesbar) oder ob auf
Endpoint-Layern stattdessen nur die Meta-Tags zählen sollen, ist Geschmacks- und
Governance-Frage — beides ist heute pro Layer konfigurierbar (→ A6).

---

## 4. Abstimmungspunkte mit allen Domänen-Teams

### A. Konventionen (Governance-Entscheidungen, kein/kaum Code)

**A1 — Endpoint-Kontrakt-Standard.**
Alle Teams aktivieren auf ihrem Data-Product-Layer das dokumentierte Muster:
`annotate-column-origin-infos: always` + `write-cll-tags-to-meta: true`
(+ `annotation-include-source-description: false`). Damit trägt jede veröffentlichte
Spalte ihre Herkunft im Manifest.
*Zu entscheiden: Ist das verbindlicher Standard für alle Data Products?*

**A2 — Manifest-Austausch zwischen Domänen.**
Die Provenienz kann nur über Repo-Grenzen fließen, wenn Konsumenten Zugriff auf das
`manifest.json` des Produzenten haben (CI veröffentlicht das Artefakt, Konsumenten
ziehen es versioniert).
*Zu entscheiden: Ablageort (Artefakt-Feed/Storage), Versionierung, Zugriffsmodell.*

**A3 — Qualifizierte Provenienz über Repo-Grenzen.**
Herkunftsverweise sind heute repo-lokal (`MODELL.SPALTE`). Im Mesh können
Modellnamen zwischen Domänen kollidieren. Grenzüberschreitende Verweise müssen
qualifiziert werden; technischer Schlüssel für das Matching:
`(database, schema, identifier)` — der Mechanismus existiert im Tool bereits repo-intern.
*Zu entscheiden: Namenskonvention für domänenübergreifende Herkunftsverweise.*

**A4 — Zweistufiges Glossar.**
Mesh-weite Standard-/Auditspalten (z. B. technische Batch-Zeitstempel) gehören in ein
**zentrales Glossar** (eigenes Repo, Ownership bei der Governance), das jedes Team
zusätzlich zu seinem lokalen Glossar einbindet (lokal gewinnt). Kleine Tool-Anpassung
nötig (mehrere Glossar-Pfade statt einem). Abgrenzung zum Atlan-Glossar:
Spaltenbeschreibungs-Glossar lebt als Code im Git (wird von osmosis-cll verarbeitet),
das Business-Glossar lebt in Atlan; eine Verknüpfung über Namenskonvention oder
Meta-Tag ist möglich.
*Zu entscheiden: Ownership, Pflegeprozess, Inhalt — und führendes System je Glossartyp.*

**A5 — Schreibrichtung: dbt-Repo als Single Source of Truth für Spaltenbeschreibungen.**
Beschreibungen entstehen und ändern sich im dbt-Repo (Docs-as-Code, reviewbar,
versioniert) und fließen über das Manifest nach Atlan — nicht umgekehrt. Direkte
Beschreibungs-Edits in Atlan an dbt-Spalten würden beim nächsten Ingest überschrieben
oder erzeugen Drift.
*Zu entscheiden: Verbindliche Schreibrichtung; Prozess für Korrekturwünsche aus dem
Katalog zurück ins Repo (z. B. Ticket/PR statt Katalog-Edit).*

**A6 — Mapping der Provenienz-Metadaten nach Atlan.**
Die Meta-Tags (`renamed_from` / `derived_from` / `computed_in`, `desc-source`) und
die Trust-Klassen aus doc-health sollen in Atlan als Custom Metadata / Badges
erscheinen, damit Katalognutzer Herkunft und Vertrauensgrad sehen. Zusätzlich zu
klären: Sollen die menschenlesbaren Annotations-Blöcke im Beschreibungstext auch im
Katalog erscheinen, oder gelten auf Endpoint-Layern nur die Meta-Tags
(`annotation-include-source-description: false` ist bereits Teil von A1)?
*Zu entscheiden: Custom-Metadata-Mapping in Atlan; Darstellung der Annotationen;
optional ein doc-health-Mindeststand als Voraussetzung für den Atlan-Ingest.*

### B. Tool-Erweiterungen (Code, in Priorisierungsreihenfolge)

**B1 — Source-Sync aus dem Upstream-Manifest (höchster Hebel).**
Neues Kommando, das die Source-YAMLs des Konsumenten-Repos aus dem Manifest des
Produzenten synchronisiert — Beschreibungen **und** Provenienz-Meta. Ergebnis: Eine
Spalte im Konsumenten-Repo lässt sich bis zu ihrem wahren Ursprung **zwei oder mehr
Repos upstream** zurückverfolgen. Heute kommen Source-Beschreibungen nur aus
DB-Kommentaren — die transportieren keine Provenienz.

**B2 — Mesh-weites Trust-Reporting inkl. Source-Coverage.**
doc-health pro Repo ist CI-fertig; Aggregation über alle Repos ist trivial. Neue
Kennzahl **Source-Coverage**: Wie viel Prozent der konsumierten Spalten (= der Vertrag
des Upstream-Teams) kommen dokumentiert an? Das ist die Zahl, mit der Teams ihre
Upstream-Qualität einfordern können.
*Zu entscheiden: Wer aggregiert, welche Mindestwerte gelten als Gate?*

**B3 — Explorer-Ausbau für Team-Nutzung.**
Vor gemeinsamem Hosting: Concurrency-Fix (der Server hält derzeit einen globalen
Graph-Zustand — faktisch Einzelnutzer). Danach: Kanten nach Transformationsart
einfärben, Trust-/Coverage-Overlay, strukturierte Herkunftsanzeige mit
„Zum Ursprung springen“, Impact-Liste als CSV-Export für Change Reviews,
projektweite Spaltensuche, teilbare Links.

**B4 — Föderierter Lineage-Explorer (mittelfristig, durch Atlan relativiert).**
Mehrere Manifeste laden und Sources mit den produzierenden Modellen über
`(database, schema, identifier)` verknüpfen → echte mesh-weite Spalten-Lineage in
einer Ansicht. Gut abgegrenzt, baut auf B1/A2/A3 auf. **Aber:** Wenn Atlan die
mesh-weite Lineage-Sicht abdeckt (Abschnitt 3), sinkt die Priorität — der Explorer
bleibt dann bewusst das repo-lokale Engineering-Werkzeug.

### C. Betrieb & Verteilung

- **Interner Artefakt-Feed** statt Installation aus einem persönlichen GitHub-Repo;
  feste Versionierung, Changelog.
- **CI-Vorlagen** für übernehmende Teams: doc-health-Gate, optionaler
  Golden-Corpus-Test gegen das eigene Repo (per Umgebungsvariable, ohne dass
  internes SQL ins Tool-Repo gelangt).
- **Public-Repo-Hygiene:** Das Tool-Repo ist öffentlich. Verbindliche Regel für alle
  Beiträge: keine internen Bezeichner, Fixtures nur anonymisiert (Vorgehen ist im
  Repo etabliert und dokumentiert).

---

## 5. Vorschlag für die Reihenfolge

1. **A1 + A2 + A5** beschließen (reine Konventionen — schalten alles Weitere frei;
   A5 sichert das Zusammenspiel mit Atlan von Anfang an ab).
2. **B1** bauen (Source-Sync) — macht das Tool mesh-nativ.
3. **A3 + A4 + A6** parallel ausarbeiten (Namenskonvention, Glossare,
   Atlan-Custom-Metadata-Mapping).
4. **B2** (Trust-Aggregation + Source-Coverage) als gemeinsames Governance-Dashboard —
   optional als Mindeststand vor dem Atlan-Ingest.
5. **B3** vor einem gemeinsam gehosteten Explorer; **B4** nur, falls Atlan die
   mesh-weite Spalten-Lineage nicht ausreichend abdeckt.
