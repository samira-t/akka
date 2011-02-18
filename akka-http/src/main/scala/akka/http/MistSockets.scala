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
    log.slf4j.info("Supporting Jetty web sockets (experimental).")
  }

  protected override def doWebSocketConnect(request:HttpServletRequest, protocol:String):WebSocket = MistSocket(request)
	
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

/*
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
*/

trait WebSocketProducer
{
  this:Actor =>

  import akka.serialization.Serializable.ScalaJSON
  import akka.serialization.Serializable.SBinary
  import sbinary.DefaultProtocol._

  final val TextFrame = 0x4
  final val BinaryFrame = 0x5

  protected def handleWebSocketRequest:Receive =
  {
    case Connect(_,consumer) => become (_wsrecv(consumer))
  }

  private def _wsrecv(socket:WebSocket.Outbound):Receive =
  {
    case Disconnect => unbecome
    case f:Frame => socket sendMessage (BinaryFrame, f data, 0, f length)
    case s:String => socket sendMessage s
    case a:Array[Byte] => socket sendMessage (BinaryFrame, a, 0, a size)
    case sj:ScalaJSON[_] => socket sendMessage sj.toJSON
    case sb:SBinary[_] =>
      val sbin = sb.toBytes
      socket sendMessage (BinaryFrame, sbin, 0, sbin size)
    case any:Any => socket sendMessage any.toString
  }
}

class MistSocket(request:HttpServletRequest) extends WebSocket with Mist
{
  override def onConnect(outbound:WebSocket.Outbound) = _root ! Connect(request, outbound)
  override def onDisconnect = _root ! Disconnect(request)

  def onFragment(more:Boolean, opcode:Byte, data:Array[Byte], offset:Int, length:Int) = _root ! Frame(request, data, length)
  def onMessage(opcode:Byte, data:Array[Byte], offset:Int, length:Int) = _root ! Frame(request, data, length)
  def onMessage(opcode:Byte, data:String) = _root ! Frame(request, data getBytes, data length)
}

object MistSocket
{
  def apply(request:HttpServletRequest) = new MistSocket(request)
}

class WebSocketRequest(req:HttpServletRequest) extends RequestMethod
{
  val builder = null
  val context = None
  val go = false
  def timeout(ms:Long) = false
  val suspended = false

  override def request = req
  override def response = null
}

case class Connect(req:HttpServletRequest, consumer:WebSocket.Outbound) extends WebSocketRequest(req)
case class Disconnect(req:HttpServletRequest) extends WebSocketRequest(req)
case class Frame(req:HttpServletRequest, data:Array[Byte], length:Int) extends WebSocketRequest(req)

