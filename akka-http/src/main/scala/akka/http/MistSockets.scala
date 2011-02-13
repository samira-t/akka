/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.http

/**
 * EXPERIMENTAL implementation of web sockets in mist using Jetty WebsocketServlet (for now)...
 * - expect change
 *
 * @author Garrick Evans
 */

import akka.actor._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.eclipse.jetty.websocket._


class AkkaMistWebsocketServlet extends WebSocketServlet with Mist
{
  import javax.servlet.ServletConfig

  override def init(config: ServletConfig) {
    super.init(config)
    initMist(config.getServletContext)
    log.slf4j.info("Supporting Jetty web sockets.")
  }

  protected override def doWebSocketConnect(request:HttpServletRequest, protocol:String):WebSocket = new MistSocket(request, protocol)
	
  //protected override def checkOrigin(request:HttpServletRequest, host:String, origin:String):String
  //protected override def service(request:HttpServletRequest, response:HttpServletResponse) 
  protected override def  doDelete(req: HttpServletRequest, res: HttpServletResponse) = mistify(req, res)(_factory.get.Delete)
  protected override def     doGet(req: HttpServletRequest, res: HttpServletResponse) = mistify(req, res)(_factory.get.Get)
  protected override def    doHead(req: HttpServletRequest, res: HttpServletResponse) = mistify(req, res)(_factory.get.Head)
  protected override def doOptions(req: HttpServletRequest, res: HttpServletResponse) = mistify(req, res)(_factory.get.Options)
  protected override def    doPost(req: HttpServletRequest, res: HttpServletResponse) = mistify(req, res)(_factory.get.Post)
  protected override def     doPut(req: HttpServletRequest, res: HttpServletResponse) = mistify(req, res)(_factory.get.Put)
  protected override def   doTrace(req: HttpServletRequest, res: HttpServletResponse) = mistify(req, res)(_factory.get.Trace)
	
}

class MistSocketActor extends Actor
{
	private var _outbound:Option[WebSocket.Outbound] = None
	
	def receive = 
	{
		case outbound:WebSocket.Outbound => _outbound = Some(outbound)
		case in:Tuple2[ActorRef, Frame] => in._1 ! in._2
		case out:Frame => 
			_outbound match {
				case Some(socket) => socket.sendMessage(out.opcode, out.data, out.offset, out.length)
				case None => log.slf4j.info("No socket connected, cannot send data. Reply will be lost.")
			}
		case MistSocket.Disconnect =>
			_outbound match {
				case Some(socket) => {
					socket.disconnect
					log.slf4j.debug("Socket disconnect by request.")
				}
				case None => log.slf4j.info("No socket connected, cannot disconnect.")
			}
		case _ => {}
	}
	
}


class MistSocket(hsr:HttpServletRequest, protocol:String) extends WebSocket with Mist
{
	private var _msa:ActorRef = Actor.actorOf[MistSocketActor].start
	
    override def onConnect(outbound:WebSocket.Outbound) = _msa ! outbound
    override def onDisconnect = _msa stop
    
    def onFragment(more:Boolean, opcode:Byte, data:Array[Byte], offset:Int, length:Int) = 
      _msa ! Tuple2(_root, Frame(_msa, hsr, protocol, more, opcode, data, offset, length))

    def onMessage(opcode:Byte, data:Array[Byte], offset:Int, length:Int) = 
      _msa ! Tuple2(_root, Frame(_msa, hsr, protocol, false, opcode, data, offset, length))
		
    def onMessage(opcode:Byte, data:String) =
      _msa ! Tuple2(_root, Frame(_msa, hsr, protocol, false, opcode, data.getBytes, 0, data.length))
}

object MistSocket
{
  case object Disconnect
  def apply(hsr:HttpServletRequest, protocol:String) = new MistSocket(hsr, protocol)
}


case class Frame(socket:ActorRef, hsr:HttpServletRequest, protocol:String, more:Boolean, opcode:Byte, data:Array[Byte], offset:Int, length:Int) extends RequestMethod
{
  val builder = null
  val context = None
  val go = false
  def timeout(ms:Long) = false
  val suspended = false

  override def request = hsr
  override def response = null

  def this(socket:ActorRef, frame:Frame, data:Array[Byte], length:Int) = 
    this(socket, frame.hsr, frame.protocol, frame.more, frame.opcode, data, 0, length)
}

