#!/usr/local/bin/ruby
# frozen_string_literal: true

module Fluent
    
      class Kubelet_Health_Input < Input
        Plugin.register_input('dockerhealth', self)
    
        def initialize
          super
          require 'yaml'
          require 'json'
    
          require_relative 'KubernetesApiClient'
          require_relative 'oms_common'
          require_relative 'omslog'
          require_relative 'ApplicationInsightsUtility'
          require_relative 'DockerApiClient'
        end
    
        config_param :run_interval, :time, :default => '1m'
        config_param :tag, :string, :default => "oms.containerinsights.DockerHealth"
    
        def configure (conf)
          super
        end
    
        def start
          if @run_interval
            @finished = false
            @condition = ConditionVariable.new
            @mutex = Mutex.new
            @thread = Thread.new(&method(:run_periodic))
            @@previousDockerState = ""
            # Tracks the last time docker health data sent for each node
            @@dockerHealthDataTimeTracker = DateTime.now.to_time.to_i
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
    
        #def populateCommonfields(record)
         # record['ClusterName'] = KubernetesApiClient.getClusterName
         # record['ClusterId'] = KubernetesApiClient.getClusterId
         # record['ClusterRegion'] = KubernetesApiClient.getClusterRegion
        #end

        def enumerate
          begin
            currentTime = Time.now
            emitTime = currentTime.to_f
            batchTime = currentTime.utc.iso8601
            record = {}
            eventStream = MultiEventStream.new
            $log.info("in_docker_health::Making a call to get docker info @ #{Time.now.utc.iso8601}")
            isDockerStateFlush = false
            dockerInfo = DockerApiClient.dockerInfo
            if (!dockerInfo.nil? && !dockerInfo.empty?)
              dockerState = 'Healthy'
            else
              dockerState = 'Unhealthy'
            end
            currentTime = DateTime.now.to_time.to_i
            timeDifference =  (currentTime - @@dockerHealthDataTimeTracker).abs
            timeDifferenceInMinutes = timeDifference/60
            if (timeDifferenceInMinutes >= 3) || 
               !(dockerState.casecmp(@@previousDockerState) == 0)
              previousDockerState = dockerState
              isDockerStateFlush = true
              @@dockerHealthDataTimeTracker = currentTime
              record['CollectionTime'] = batchTime #This is the time that is mapped to become TimeGenerated
              record['DockerState'] = dockerState
              hostName = (OMS::Common.get_hostname)
              record['Computer'] = hostName
              eventStream.add(emitTime, record) if record
            end

            if isDockerStateFlush
              router.emit_stream(@tag, eventStream) if eventStream
            end
          rescue  => errorStr
                #$log.warn line.dump, error: errorStr.to_s
                #$log.debug_backtrace(e.backtrace)
                $log.warn("error : #{errorStr.to_s}")
                #ApplicationInsightsUtility.sendExceptionTelemetry(errorStr)
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
                $log.info("in_health_docker::run_periodic @ #{Time.now.utc.iso8601}")
                enumerate
              rescue => errorStr
                $log.warn "in_health_docker::run_periodic: enumerate Failed for docker health: #{errorStr}"
                ApplicationInsightsUtility.sendExceptionTelemetry(errorStr)
              end
            end
            @mutex.lock
          end
          @mutex.unlock
        end
    
      end # Health_Docker_Input
    
    end # module
    
    