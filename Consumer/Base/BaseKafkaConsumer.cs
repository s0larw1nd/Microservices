using Common;
using Confluent.Kafka;
using Consumer.Config;
using Microsoft.Extensions.Options;

namespace Consumer.Base;

public abstract class BaseKafkaConsumer<T>: IHostedService
    where T : class
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<BaseKafkaConsumer<T>> _logger;
    private readonly string _topic;
    private Timer _batchTimer;
    private TimeSpan batchTimeout;
    private List<ConsumeResult<string,string>> _messageBuffer;
    private KafkaSettings kafkaSettings;
    private SemaphoreSlim _processingSemaphore;
    
    protected BaseKafkaConsumer(
        IOptions<KafkaSettings> kafkaSett,
        string topic,
        ILogger<BaseKafkaConsumer<T>> logger)
    {
        kafkaSettings = kafkaSett.Value;
        
        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaSettings.BootstrapServers,
            GroupId = kafkaSettings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 5_000,
            SessionTimeoutMs = 60_000,
            HeartbeatIntervalMs = 3_000,
            MaxPollIntervalMs = 300_000,
        };

        _logger = logger;
        _topic = topic;
        _consumer = new ConsumerBuilder<string, string>(config).Build();
        
        batchTimeout = TimeSpan.FromSeconds(kafkaSettings.CollectTimeoutMs);
        _processingSemaphore = new SemaphoreSlim(1, 1);
        _messageBuffer = new List<ConsumeResult<string,string>>();
    }
    
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await StartConsuming(_topic, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        StopConsuming();
        return Task.CompletedTask;
    }

    private async Task StartConsuming(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        _logger.LogInformation($"Started consuming from topic: {topic}");
        
        _batchTimer = new Timer(ProcessBatchByTimeout, null, batchTimeout, batchTimeout);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                
                if (consumeResult.Message != null)
                {
                    await _processingSemaphore.WaitAsync();
                    try
                    {
                        _messageBuffer.Add(consumeResult);
                        
                        if (_messageBuffer.Count >= kafkaSettings.CollectBatchSize)
                        {
                            await ProcessBatch();
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "Error processing message");
                    }
                    _processingSemaphore.Release();
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer cancelled");
        }
        catch (ConsumeException ex)
        {
            _logger.LogError(ex, "Consume error occurred");
        }
        finally
        {
            StopConsuming();
        }
    }
    
    private void StopConsuming()
    {
        _logger.LogInformation($"Stopping consuming from topic: {_topic}");
        _consumer.Close();
        _consumer.Dispose();
        _processingSemaphore?.Dispose();
    }
    
    private async Task ProcessBatch()
    {
        if (_messageBuffer.Count == 0) return;

        var currentBatch = _messageBuffer
            .Select(cr => new Message<T>
            {
                Key = cr.Message.Key,
                Body = cr.Message.Value.FromJson<T>()
            })
            .ToArray();

        try
        {
            await ProcessMessages(currentBatch);
            foreach (var message in _messageBuffer)
            {
                _consumer.Commit(message);
            }
            Console.WriteLine($"Successfully processed batch of {currentBatch.Length} messages");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to process batch: {ex.Message}");
        }
        
        _messageBuffer.Clear();
    }
    
    private async void ProcessBatchByTimeout(object state)
    {
        await _processingSemaphore.WaitAsync();
        
        if (_messageBuffer.Count > 0)
        {
            await ProcessBatch();
        }
        
        _processingSemaphore.Release();
    }
    
    protected abstract Task ProcessMessages(Message<T>[] messages);
}