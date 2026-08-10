# SqlCdc

Change Data Capture (CDC) in tempo reale per **SQL Server**, basato sul CDC nativo.
Il package interroga le capture instance (`cdc.fn_cdc_get_all_changes_*`), ricostruisce gli eventi
(insert/update/delete con immagini *before*/*after*) e li emette su un `System.Threading.Channels.Channel<CdcChange>`.

## Caratteristiche

- CDC nativo SQL Server: nessuna colonna aggiunta alle tabelle sorgente.
- Eventi ricchi: immagine **before/after**, tipo operazione, LSN, commit time e **update mask**.
- Watermark LSN persistito: il watcher riprende esattamente da dove si era fermato.
- Channel bounded con backpressure: il consumer lento blocca il poller, niente perdita in memoria.
- API fluente (`SqlCdcWatcherBuilder`).

## Prerequisiti

- SQL Server (2016+) con CDC abilitato su database e tabelle:

```sql
-- Richiede sysadmin o db_owner
EXEC sys.sp_cdc_enable_db;

EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name   = N'Orders',
     @role_name     = NULL;
```

## Utilizzo

```csharp
using SqlCdc;

var watcher = SqlCdcWatcherBuilder
    .Create()
    .UseConnectionString("Server=.;Database=MyDb;TrustServerCertificate=True")
    .WatchTable("dbo", "Orders")
    .WatchTable("dbo", "Customers")
    .WithPollInterval(TimeSpan.FromMilliseconds(250))
    .UseStateStore(new SqlCdcStateStore(connectionString))  // resume dopo riavvio
    .Build();

await watcher.StartAsync(cts.Token);

await foreach (var change in watcher.Changes.WithCancellation(cts.Token))
{
    Console.WriteLine($"{change.TableName} {change.Operation}");
    foreach (var (column, value) in change.After)
        Console.WriteLine($"  {column} = {value}");
}
```

### ASP.NET Core / Generic Host

```csharp
builder.Services.AddSqlCdc(cdc => cdc
    .UseConnectionString(connectionString)
    .WatchTable("dbo", "Orders")
    .UseStateStore(new SqlCdcStateStore(connectionString)));

builder.Services.AddCdcChangeHandler<OrderChangedHandler>();
```

`AddSqlCdc` registra il watcher come singleton e un `IHostedService` che lo avvia con l'host e lo
ferma allo shutdown. Logger e `ICdcStateStore` vengono presi dal container se registrati; quello che
imposti nel delegato ha la precedenza.

```csharp
public sealed class OrderChangedHandler(AppDbContext db) : ICdcChangeHandler
{
    public async Task HandleAsync(CdcChange change, CancellationToken ct = default)
    {
        // ...
    }
}
```

Ogni handler è **scoped** e viene risolto in uno scope dedicato per ciascun evento, quindi puoi
iniettare un `DbContext` senza condividerlo tra eventi. Gli handler registrati ricevono tutti ogni
evento, in ordine di registrazione. Se un handler solleva un'eccezione, l'errore viene loggato e
l'evento **scartato**: il watermark avanza quando l'evento raggiunge il channel, non quando viene
gestito, quindi il retry è responsabilità dell'handler.

Senza handler registrati il watcher viene comunque avviato dall'host: inietta `SqlCdcWatcher` e
consuma `Changes` direttamente.

### Modello evento

```csharp
record CdcChange
{
    string CaptureInstance;
    string SourceSchema, SourceTable;
    CdcOperationType Operation;        // Insert | Update | Delete
    byte[] StartLsn, SeqVal;
    DateTime CommitTime;
    IReadOnlyDictionary<string, object?> Before;
    IReadOnlyDictionary<string, object?> After;
    IReadOnlyDictionary<string, bool> UpdateMask;  // colonne modificate (solo update)
    string Key;                                     // id stabile per change
}
```

## Opzioni

| Metodo builder | Default | Descrizione |
|---|---|---|
| `WithPollInterval` | 500 ms | Frequenza di polling delle capture instance |
| `WithBatchSize` | 1000 | Rows per ciclo per tabella (cap *soft*, vedi sotto) |
| `WithChannelCapacity` | 100 000 | Capacità del channel (backpressure) |
| `StartFrom` | `FromNow` | `FromNow` (dal max LSN corrente) o `FromBeginning` per lo storico |
| `UseStateStore` | in-memory | `SqlCdcStateStore` per persistere il watermark LSN |
| `WithRetryDelay` | 5 s | Attesa dopo un errore di polling |

## Semantica di consegna

I batch vengono sempre tagliati su un confine di transazione (LSN), così le immagini *before* e
*after* di un update restano insieme. Per questo `WithBatchSize` è un cap **soft**: una singola
transazione più grande del batch viene letta comunque per intero (con un warning nei log).

Il channel è **bounded**: se il consumer non consuma, il poller si mette in attesa
(`BoundedChannelFullMode.Wait`). Il watermark LSN viene salvato dopo ogni batch completato,
per cui gli eventi sono consegnati **at-least-once**: in caso di crash un evento può essere riemesso
alla ripresa. I consumer dovrebbero deduplicare usando `CdcChange.Key` se necessario.

### Retention CDC

Il cleanup job di SQL Server elimina le change table oltre la retention configurata (3 giorni di
default). Se il servizio resta fermo più a lungo, il watermark salvato punta a righe che non esistono
più: il watcher **riparte dal più vecchio LSN ancora disponibile** ed emette un warning nei log.
Le modifiche nel mezzo sono perse — è una perdita di dati inevitabile, ma esplicita e non un errore
in loop. Per finestre di fermo più lunghe, alza la retention:
`EXEC sys.sp_cdc_change_job @job_type='cleanup', @retention=<minuti>`.

### Isolamento degli errori

Ogni tabella ha il proprio stato di errore: se una capture instance fallisce (permessi, CDC
disabilitato, tabella rimossa), le altre continuano a essere pollate normalmente. La tabella in
errore viene riprovata dopo `WithRetryDelay`, non ad ogni ciclo di polling.

## Setup sviluppo

```bash
dotnet restore
dotnet build SqlCdc.slnx
dotnet test tests/SqlCdc.Tests                    # unit test, nessuna dipendenza esterna
dotnet test tests/SqlCdc.IntegrationTests         # richiede Docker (vedi sotto)
dotnet pack src/SqlCdc/SqlCdc.csproj -c Release   # genera il package NuGet
```

### Test di integrazione

Girano contro un SQL Server reale avviato con [Testcontainers](https://dotnet.testcontainers.org/):
serve **Docker** in esecuzione. Il container parte con SQL Server Agent abilitato, perché senza Agent
il capture job di CDC non gira e le change table restano vuote. Su Apple Silicon l'immagine è amd64 e
viene eseguita in emulazione (serve Rosetta abilitata in Docker Desktop).

Un run completo richiede circa 30 secondi più il primo avvio del container. Per escluderli:

```bash
dotnet test SqlCdc.slnx --filter "Category!=Integration"
```

## Sample

```bash
SQLCDC_CONNECTION="Server=.;Database=MyDb;User Id=sa;Password=...;TrustServerCertificate=True" \
  dotnet run --project samples/SqlCdc.Sample
```

Vedi `scripts/enable-cdc.sql` per abilitare CDC su una tabella di prova.
