# Recon Sniper

Standalone event-driven MNQ engine for Willie. **Not TradeChampion.** Own HUD, own NinjaTrader bridge on port **5564**.

Chart overlay is **lines only** (gold entry, red stop, green target, purple trail). No price/bias/STOP/ENTRY/TARGET words on the chart.

Optional **8TCM** pack (Settings, default **OFF**) runs next to EMA 9/20/50. One live trade at a time — neither system interrupts the other. With 8TCM on, both packs use the 8TCM hold: key-level green target, then purple trail. 8TCM shorts stay off unless you flip that checkbox.

HUD **GOAL** (±$25) and **CLOCK** (±15m) control the daily hunt. With goal on, entry gates open so the bot can trade; clock pressure tightens as time runs down. **RESET PNL** zeros the session and restarts the clock.

Optional **Recon tip trail** (HUD GATES — default **OFF**): arms at **$15 total** open profit, purple tip trail 5.5 pts behind the tip (never red after arm), four chart lines (gold / green / red hard stop / purple trail). Stock base stays the $25/contract bank.

## What it does

- Watches live ticks, not candle-close entries
- Trades only when the tape is **TRENDING** (or high-vol trend). Chop sits out
- Observe / Paper / Live from the Night Shell HUD
- HUD size control: **MNQ contracts** — 1 banks **$25**, 2 banks **$50**
- HUD **STOP** in points (default 20). Dollars scale with contract size
- After the bank is tagged it is a **floor**, not an exit. A **4.5-point trail** sits behind the tip and never gives the bank back
- Large **PNL** (session net, including open) and **BALANCE** (NinjaTrader equity) on the HUD. Balance shows **WAITING** until ReconSniperBridge heartbeats include cash/equity
- PNL LOG lists closed trades plus the live open row. Chart stays **lines only** (no Draw.Text)

## Willie PC install

1. Clone this repo
2. `py -3 -m pip install -r requirements.txt`
3. Copy `mark2/bridge/ReconSniperBridge.cs` into  
   `Documents\NinjaTrader 8\bin\Custom\Strategies\`
4. NinjaScript Editor → **F5**
5. Add **ReconSniperBridge** to the MNQ chart (Sim101 or live). Leave TradeChampion on **5560** alone
6. Double-click `START-RECON-SNIPER.bat` — same launcher as Brent’s desktop: HUD in the background plus the 8TCM board window
7. Optional: double-click `CREATE-RECON-SNIPER-SHORTCUT.bat` to put **ReconSniper** on the Desktop

Default mode is **OBSERVE_ONLY** (simulated runner on the HUD, no NT orders). Switch to Paper or Live from Control.

## After pulling an update

1. Copy `mark2/bridge/ReconSniperBridge.cs` into `Documents\NinjaTrader 8\bin\Custom\Strategies\` (overwrite)
2. NinjaScript Editor → **F5** (needed for NT balance to flow)
3. Restart `START-RECON-SNIPER.bat`

## Safety

- Does not import ImpulseRuntime / TradeChampion / port 5560
- Flatten from the HUD cuts the local (and live) position
