module Dynflow
  class MicroActor
    include Algebrick::TypeCheck
    include Algebrick::Matching

    attr_reader :logger, :initialized

    Terminate = Algebrick.atom

    def initialize(logger, *args)
      @logger      = logger
      @initialized = Future.new
      @thread      = Thread.new { run *args }
      Thread.pass until @mailbox
    end

    def <<(message)
      raise 'actor terminated' if terminated?
      @mailbox << [message, nil]
      self
    end

    def ask(message, future = Future.new)
      future.fail Dynflow::Error.new('actor terminated') if terminated?
      @mailbox << [message, future]
      future
    end

    def stopped?
      @terminated.ready?
    end

    private

    def delayed_initialize(*args)
    end

    def termination
      terminate!
    end

    def terminating?
      @terminated
    end

    def terminated?
      terminating? && @terminated.ready?
    end

    def terminate!
      raise unless Thread.current == @thread
      @terminated.resolve true
      throw Terminate
    end

    def on_message(message)
      raise NotImplementedError
    end

    def receive
      message, future = @mailbox.pop
      #logger.debug "#{self.class} received:\n  #{message}"
      if message == Terminate
        # TODO do not use this future to store in @terminated use one added to Terminate message
        if terminating?
          @terminated.do_then { future.resolve true } if future
        else
          @terminated = (future || Future.new)
          termination
        end
      else
        on_envelope message, future
      end
    rescue => error
      logger.fatal error
    end

    def on_envelope(message, future)
      if future
        future.evaluate_to { on_message message }
      else
        on_message message
      end
      if future && future.failed?
        logger.error future.value
      end
    end

    def run(*args)
      Thread.current.abort_on_exception = true

      @mailbox    = Queue.new
      @terminated = nil

      delayed_initialize(*args)
      Thread.pass until @initialized
      @initialized.resolve true

      catch(Terminate) { loop { receive } }
    end
  end
end
