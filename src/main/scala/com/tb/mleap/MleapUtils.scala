package com.tb.mleap

import com.tb.data.Iris
import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport.MleapBundleFileOps
import resource.managed

import scala.util.{Failure, Success}

object MleapUtils {

  def loadMleapModel(path: String) = {

    val f = new java.io.File(path)

    (for (file <- managed(BundleFile(f))) yield {
      file.loadMleapBundle().get.root
    }).tried match {
      case Success(transformer) => Success(transformer)
      case Failure(_) => Failure(throw new RuntimeException("Could not load transformer"))
    }
  }

}
