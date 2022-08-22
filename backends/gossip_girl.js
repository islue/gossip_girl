var util = require('util'),
    dgram = require('dgram')

function GossipGirl(startupTime, config, emitter) {
  var self = this
  self.config = config.gossip_girl || []
  self.statsd_config = config
  self.ignorable = [ "statsd.packets_received", "statsd.bad_lines_seen", "statsd.packet_process_time" ]
  self.sock = dgram.createSocket("udp4")

  emitter.on('flush', function(time_stamp, metrics) { self.process(time_stamp, metrics); })
}

GossipGirl.prototype.gossip = function(packet, host, port) {
  var self = this
  var buffer = Buffer.from(packet)
  self.sock.send(buffer, 0, buffer.length, port, host, function(err,bytes) {
    if (err) {
      console.log(err)
    }
  })
}

GossipGirl.prototype.format = function (key, value, suffix) {
  return "'" + key + "':" + value + "|" + suffix + "\n"
}

GossipGirl.prototype.process = function(time_stamp, metrics) {
  var self = this
  hosts = self.config
  var stats, packetTmp

  var stats_map = {
    counters: { data: metrics.counters, suffix: "c",  name: "counter" },
    gauges:   { data: metrics.gauges,   suffix: "g",  name: "gauge" },
    timers:   { data: metrics.timers,   suffix: "ms", name: "timer" }
  }

  for (var i = 0; i < hosts.length; i++) {
    var packet = "", countStats = 0, countPacket = 0, countValues = 0

    for (type in stats_map) {
      stats = stats_map[type]
      for (key in stats.data) {
        if (self.ignorable.indexOf(key) >= 0) continue

        if(type == 'timers') {
          metric_data = stats.data[key]
        } else {
          metric_data = [stats.data[key]]
        }

        for(var j = 0; j < metric_data.length; j++) {
          packetTmp = self.format(key, metric_data[j], stats.suffix)
          if (self.statsd_config.dumpMessages) {
            util.log ("Gossiping about " + stats.name + ": " + packetTmp)
          }
          countStats++
          countValues += stats.data[key]
          if (!self.config[i].packetSize) {
            self.gossip(packetTmp, hosts[i].host, hosts[i].port);
            countPacket++
          } else {
            if (packet.length + packetTmp.length > self.config[i].packetSize) {
              self.gossip(packet, hosts[i].host, hosts[i].port)
              countPacket++
              packet = packetTmp;
            } else {
              packet += packetTmp;
            }
          }
        }
      }
    }
    if (packet.length > 0) {
      self.gossip(packet, hosts[i].host, hosts[i].port)
      countPacket++
    }
    if (self.config[i].verbose) {
      util.log ("Gossiping " + type + " : " + countStats + "(" + countValues + ") stats in " + countPacket + " packets")
    }
  }
}

GossipGirl.prototype.stop = function(cb) {
  this.sock.close();
  cb();
};

exports.init = function(startupTime, config, events) {
  var instance = new GossipGirl(startupTime, config, events)
  return true
}
