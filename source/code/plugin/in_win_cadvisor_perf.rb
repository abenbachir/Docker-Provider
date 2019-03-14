#!/usr/local/bin/ruby
# frozen_string_literal: true

module Fluent
  class Win_CAdvisor_Perf_Input < Input
    Plugin.register_input("wincadvisorperf", self)

    @@winNodes = []

    def initialize
      super
      require "yaml"
      require "json"

      require_relative "CAdvisorMetricsAPIClient"
      require_relative "KubernetesApiClient"
      require_relative "oms_common"
      require_relative "omslog"
    end

    config_param :run_interval, :time, :default => "1m"
    config_param :tag, :string, :default => "oms.api.wincadvisorperf"

    def configure(conf)
      super
    end

    def start
      if @run_interval
        @finished = false
        @condition = ConditionVariable.new
        @mutex = Mutex.new
        @thread = Thread.new(&method(:run_periodic))
        @@winNodeQueryTimeTracker = DateTime.now.to_time.to_i
      end
    end

    def shutdown
      if @run_interval
        @mutex.synchronize {
          @finished = true
          @condition.signal
        }
        @thread.join
      end
    end

    def enumerate()
      time = Time.now.to_f
      begin
        eventStream = MultiEventStream.new
        # $log.info "1"
        $log.info "in_win_cadvisor_perf : Getting windows nodes"
        timeDifference = (DateTime.now.to_time.to_i - @@winNodeQueryTimeTracker).abs
        timeDifferenceInMinutes = timeDifference / 60
        if (timeDifferenceInMinutes >= 5)
          @@winNodes = KubernetesApiClient.getWindowsNodes()
          $log.info "in_win_cadvisor_perf : Successuly got windows nodes"
        end
        # $log.info "2"
        @@winNodes.each do |winNode|
          # $log.info "3"
          metricData = CAdvisorMetricsAPIClient.getMetrics(winNode)
          # $log.info "4"
          # $log.info "windows node metric data: #{metricData}"
          metricData.each do |record|
            # $log.info "5"
            if !record.empty?
              # $log.info "6"
              record["DataType"] = "LINUX_PERF_BLOB"
              record["IPName"] = "LogManagement"
              eventStream.add(time, record) if record
              $log.info "windows node record: #{record}"
              # $log.info "7"
            end
          end
          router.emit_stream(@tag, eventStream) if eventStream
          # $log.info "8"
          @@istestvar = ENV["ISTEST"]
          if (!@@istestvar.nil? && !@@istestvar.empty? && @@istestvar.casecmp("true") == 0 && eventStream.count > 0)
            $log.info("winCAdvisorPerfEmitStreamSuccess @ #{Time.now.utc.iso8601}")
          end
        end
      rescue => errorStr
        $log.warn "Failed to retrieve cadvisor metric data for windows nodes: #{errorStr}"
        $log.debug_backtrace(errorStr.backtrace)
      end
    end

    def run_periodic
      @mutex.lock
      done = @finished
      until done
        @condition.wait(@mutex, @run_interval)
        done = @finished
        @mutex.unlock
        if !done
          begin
            $log.info("in_win_cadvisor_perf::run_periodic @ #{Time.now.utc.iso8601}")
            enumerate
          rescue => errorStr
            $log.warn "in_win_cadvisor_perf::run_periodic: enumerate Failed to retrieve cadvisor perf metrics for windows nodes: #{errorStr}"
          end
        end
        @mutex.lock
      end
      @mutex.unlock
    end
  end # Win_CAdvisor_Perf_Input
end # module
