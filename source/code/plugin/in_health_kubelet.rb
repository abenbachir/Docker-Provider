#!/usr/local/bin/ruby
# frozen_string_literal: true

module Fluent
    
      class Kubelet_Health_Input < Input
        Plugin.register_input('kubelethealth', self)
    
        def initialize
          super
          require 'yaml'
          require 'json'
    
          require_relative 'KubernetesApiClient'
          require_relative 'oms_common'
          require_relative 'omslog'
          require_relative 'ApplicationInsightsUtility'

        end
    
        config_param :run_interval, :time, :default => '1m'
        config_param :tag, :string, :default => "oms.containerinsights.KubeletHealth"
    
        def configure (conf)
          super
        end
    
        def start
          if @run_interval
            @finished = false
            @condition = ConditionVariable.new
            @mutex = Mutex.new
            @thread = Thread.new(&method(:run_periodic))
            @@healthTimeTracker = DateTime.now.to_time.to_i
            @@previousnetworkUnavailableStatus = ''
            @@previousOutOfDiskStatus = ''
            @@previousMemoryPressureStatus = ''
            @@previoudDiskPressureStatus = ''
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
    
        def enumerate
            currentTime = Time.now
            emitTime = currentTime.to_f
            batchTime = currentTime.utc.iso8601
            $log.info("in_health_health::enumerate : Getting nodes from Kube API @ #{Time.now.utc.iso8601}")
            nodeInventory = JSON.parse(KubernetesApiClient.getKubeResourceInfo('nodes').body)
            $log.info("in_health_health::enumerate : Done getting nodes from Kube API @ #{Time.now.utc.iso8601}")
              begin
                if(!nodeInventory.empty?)
                  eventStream = MultiEventStream.new
                  #containerNodeInventoryEventStream = MultiEventStream.new 
                    #get node inventory 
                    nodeInventory['items'].each do |item|
                        record = {}
                        record['CollectionTime'] = batchTime #This is the time that is mapped to become TimeGenerated
                        record['Computer'] = item['metadata']['name'] 
                        record['ClusterName'] = KubernetesApiClient.getClusterName
                        record['ClusterId'] = KubernetesApiClient.getClusterId
                        record['ClusterRegion'] = KubernetesApiClient.getClusterRegion
                        record['Status'] = ""

                        if item['status'].key?("conditions") && !item['status']['conditions'].empty?
                          allNodeConditions="" 
                          item['status']['conditions'].each do |condition|
                            if condition['type'] == "Ready"
                              record['KubeletReadyStatus'] = condition['status']
                              record['KubeletStatusMessage'] = condition['message']
                            elsif condition['status'] == "True" || condition['status'] == "Unknown"
                              if !allNodeConditions.empty?
                                allNodeConditions = allNodeConditions + "," + condition['type'] + ":"  + condition['reason']
                              else
                                allNodeConditions = condition['type'] + ":" + condition['reason']
                              end
                            end
                            if !allNodeConditions.empty?
                              record['NodeStatusCondition'] = allNodeConditions
                            end
                          end
                        end 
                    end
                    # Tracking time to send node health data only on timeout or change in state
                    timeDifference =  (DateTime.now.to_time.to_i - @@healthTimeTracker).abs
                    timeDifferenceInMinutes = timeDifference/60
                    if (timeDifferenceInMinutes >= 3)
                      eventStream.add(emitTime, record) if record
                      router.emit_stream(@tag, eventStream) if eventStream
                      # Resetting timer once the node health data is sent
                      @@healthTimeTracker = DateTime.now.to_time.to_i
                    end 
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
                $log.info("in_health_kubelet::run_periodic @ #{Time.now.utc.iso8601}")
                enumerate
              rescue => errorStr
                $log.warn "in_health_kubelet::run_periodic: enumerate Failed to kubelet health: #{errorStr}"
                ApplicationInsightsUtility.sendExceptionTelemetry(errorStr)
              end
            end
            @mutex.lock
          end
          @mutex.unlock
        end
    
      end # Health_Kubelet_Input
    
    end # module
    
    